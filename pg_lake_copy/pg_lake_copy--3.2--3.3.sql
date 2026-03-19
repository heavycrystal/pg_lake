-- Upgrade script for pg_lake_copy from 3.2 to 3.3

CREATE OR REPLACE FUNCTION execute_in_pgduck(query text)
RETURNS void LANGUAGE C STRICT
AS 'MODULE_PATHNAME', 'execute_in_pgduck';

COMMENT ON FUNCTION execute_in_pgduck(text) IS
  'Execute a command on the DuckDB query engine (pgduck_server)';

REVOKE ALL ON FUNCTION execute_in_pgduck(text) FROM public;
