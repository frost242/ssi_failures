#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "utils/errcodes.h"


PG_MODULE_MAGIC;


#if PG_VERSION_NUM >= 90300
#define SSIFAIL_DUMP_FILE       "pg_stat/ssi_failures.stat"
#else
#define SSIFAIL_DUMP_FILE       "global/ssi_failures.stat"
#endif

static const uint32 SSIFAIL_FILE_HEADER = 0x12435687;

typedef struct ssifailsStruct
{
    LWLockId  lock;    /* protects access to the data structure */
    int64     occured_serialization_failures;
} ssifailsStruct;
ssifailsStruct fails;

static ssifailsStruct *ssifails = NULL;

/* saved hook address in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static emit_log_hook_type prev_emit_log_hook = NULL;

/* functions */
void	_PG_init(void);
void	_PG_fini(void);
static void serialization_fail_count_shmem_startup(void);
static void ssifails_shmem_shutdown(int code, Datum arg);
static void count_serialization_failures(ErrorData *edata);
static Size ssifails_memsize(void);
Datum pg_stat_ssi_failures(PG_FUNCTION_ARGS);

/* let's have some fun */
void
_PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
    {
        elog(ERROR, "This module can only be loaded via shared_preload_libraries");
	return;
    }

    RequestAddinShmemSpace(ssifails_memsize());
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("ssi_fails_counter", 1);
#else
    RequestAddinLWLocks(1);
#endif

    /* Install hook */
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = serialization_fail_count_shmem_startup;
    prev_emit_log_hook = emit_log_hook;
    emit_log_hook = count_serialization_failures;
}

void
_PG_fini(void)
{
    if (emit_log_hook == count_serialization_failures)
        emit_log_hook = prev_emit_log_hook;
}

static void
serialization_fail_count_shmem_startup(void)
{
    bool       found;
    FILE       *file;
    uint32     header;
    int64      temp_counter;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    ssifails = NULL;

    /* Create or attach to the shared memory state */
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    /* global access lock */
    ssifails = ShmemInitStruct("ssi_fails_counter",
                                     sizeof(ssifails_memsize()),
                                     &found);
    if (!found)
    {
#if PG_VERSION_NUM >= 90600
        ssifails->lock = &(GetNamedLWLockTranche("ssi_fails_counter"))->lock;
#else
        ssifails->lock = LWLockAssign();
#endif
    }

    LWLockRelease(AddinShmemInitLock);

    if (!IsUnderPostmaster)
        on_shmem_exit(ssifails_shmem_shutdown, (Datum) 0);

    /* Load stat file, don't care about locking */
    file = AllocateFile(SSIFAIL_DUMP_FILE, PG_BINARY_R);
    if (file == NULL)
    {
        if (errno == ENOENT)
            return;            /* ignore not-found error */
        goto error;
    }

    /* check if header is valid */
    if (fread(&header, sizeof(uint32), 1, file) != 1 ||
            header != SSIFAIL_FILE_HEADER)
        goto error;

    if (fread(&temp_counter, sizeof(int64), 1, file) != 1)
        goto error;

    ssifails->occured_serialization_failures = temp_counter;

    FreeFile(file);

    /* Remove the file so it's not included in backups/replication slaves, etc.
     * A new file will be written on next shutdown.
     */
    unlink(SSIFAIL_DUMP_FILE);

    return;

error:
    /* Don't issue anything else than FATAL, this prevents PostgreSQL to
     * start properly
     */
    ereport(LOG,
            (errcode_for_file_access(),
	     errmsg("could not read file \"%s\": %m",
                    SSIFAIL_DUMP_FILE)));
    if (file)
        FreeFile(file);

    /* delete bogus file, don't care of errors in this case */
    unlink(SSIFAIL_DUMP_FILE);
}

/*
 * shmem_shutdown hook: dump counters into file.
 *
 */
static void
ssifails_shmem_shutdown(int code, Datum arg)
{
    FILE *file;

    /* Don't try to dump during a crash. */
    if (code)
        return;

    if (!ssifails)
        return;

    file = AllocateFile(SSIFAIL_DUMP_FILE ".tmp", PG_BINARY_W);
    if (file == NULL)
        goto error;

    if (fwrite(&SSIFAIL_FILE_HEADER, sizeof(uint32), 1, file) != 1)
        goto error;

    if (fwrite(&(ssifails->occured_serialization_failures), sizeof(int64), 1, file) != 1)
        goto error;

    if (FreeFile(file))
    {
        file = NULL;
        goto error;
    }

    if (rename(SSIFAIL_DUMP_FILE ".tmp", SSIFAIL_DUMP_FILE) != 0)
        ereport(FATAL, (errcode_for_file_access(),
                        errmsg("could not rename stat file \":%s\": %m",
                               SSIFAIL_DUMP_FILE ".tmp")));
    return;

error:
    ereport(LOG, (errcode_for_file_access(),
                   errmsg("could not read file \"%s\": %m",
                          SSIFAIL_DUMP_FILE)));
    if (file)
        FreeFile(file);
    unlink(SSIFAIL_DUMP_FILE);
}

static Size
ssifails_memsize(void)
{
    Size	size;

    size = MAXALIGN(sizeof(ssifailsStruct));

    return size;
}

/*
 * The following function accounts every serialization failures encountered.
 * Serialization failures are accounted first, then other hooks are called.
 */
static void
count_serialization_failures(ErrorData *edata)
{
    /* filter out serialization failures and increment counter */
    if (edata->sqlerrcode == ERRCODE_T_R_SERIALIZATION_FAILURE) {
        LWLockAcquire(ssifails->lock, LW_EXCLUSIVE);
        ssifails->occured_serialization_failures++;
        LWLockRelease(ssifails->lock);
    }

    /* Call any previous hooks */
    if (prev_emit_log_hook)
        prev_emit_log_hook(edata);
}

/*
 * The following function returns the number of serialization failures
 * encountered.
 */
Datum
pg_stat_ssi_failures(PG_FUNCTION_ARGS)
{
    int64   count_res;

    if (!ssifails)
        ereport(ERROR,
	    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
             errmsg("pg_stat_ssi_failures must be loaded via shared_preload_libraries")));

    LWLockAcquire(ssifails->lock, LW_SHARED);
    count_res = ssifails->occured_serialization_failures;
    LWLockRelease(ssifails->lock);

    PG_RETURN_INT64(count_res);
}

/* eof */
