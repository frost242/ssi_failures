# pg_stat_ssi_failures

Little extension to account serialization failures in PostgreSQL.

The main purpose of this extension is to provide some counter that can return
some kind of perfdata to a monitoring suite.

## Functions

  * `pg_stat_ssi_failures` : returns the number of serialization failures that
     occurred in the cluster.

  * `pg_stat_ssi_failures_reset` : reset the failure counter.

## How to monitor this counter ?

In a first step, create an history table to store intermediate values :

```
CREATE TABLE ssi_failures
    AS SELECT pg_stat_ssi_failures() AS failcount;
```

This table will store the counter value


Now, you can use the following query, for example with
[check_pgactivity](https://github.com/OPMDG/check_pgactivity) :
```
WITH ssifails AS (
  SELECT pg_stat_ssi_failures() AS failcount, failcount AS last_call_failcount FROM ssi_failures
),
update_counter AS (
  UPDATE ssi_failures
     SET failcount = ssifails.failcount
    FROM ssifails
)
SELECT 0 AS dumb_status,
       failcount - last_failcount AS failures
  FROM ssifails;
```

