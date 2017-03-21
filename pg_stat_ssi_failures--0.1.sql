CREATE OR REPLACE FUNCTION pg_stat_ssi_failures()
   RETURNS bigint
   LANGUAGE c COST 1000
   AS '$libdir/pg_stat_ssi_failures', 'pg_stat_ssi_failures';


CREATE OR REPLACE FUNCTION pg_stat_ssi_failures_reset()
   RETURNS void
   LANGUAGE c COST 1000
   AS '$libdir/pg_stat_ssi_failures', 'pg_stat_ssi_failures_reset';

