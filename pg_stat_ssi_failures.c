
#include "postgres.h"

#include "access/hash.h"
#include "executor/executor.h"
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

/* TODO: see which version are supported */
#if PG_VERSION_NUM >= 90300
#define PGSK_DUMP_FILE		"pg_stat/pg_stat_kcache.stat"
#else
#define PGSK_DUMP_FILE		"global/pg_stat_kcache.stat"
#endif

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
}

/*
 * shmem_shutdown hook: dump counters into file.
 *
 */
static void
ssifails_shmem_shutdown(int code, Datum arg)
{
    /* Don't try to dump during a crash. */
    if (code)
        return;

    if (!ssifails)
        return;
}

static Size
ssifails_memsize(void)
{
    Size	size;

    size = MAXALIGN(sizeof(ssifailsStruct));

    return size;
}

static void
count_serialization_failures(ErrorData *edata)
{
    /* Call any previous hooks */
    if (prev_emit_log_hook)
        prev_emit_log_hook(edata);

    /* filter out serialization failures and increment counter */
    if (edata->sqlerrcode == ERRCODE_T_R_SERIALIZATION_FAILURE) {
        LWLockAcquire(ssifails->lock, LW_EXCLUSIVE);
        ssifails->occured_serialization_failures++;
        LWLockRelease(ssifails->lock);
    }
}

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
