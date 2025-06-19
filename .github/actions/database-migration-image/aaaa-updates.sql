set define on;
def builduser = 'DBAdmin'
def CWMS_SCHEMA = 'CWMS_20'
set echo on;
@/cwmsdb/schema/src/cwms/create_sec_triggers.sql
@/cwmsdb/schema/src/cwms/cwms_sec_pkg_body.sql
@/cwmsdb/schema/src/cwms/tables/at_ts_extents.sql
@/cwmsdb/schema/src/cwms/cwms_util_pkg_body.sql

@/cwmsdb/schema/src/cwms/types/ztvs_type.sql
@/cwmsdb/schema/src/cwms/types/ztvs_entry_type.sql
@/cwmsdb/schema/src/cwms/types/ztvs_entry_array.sql
@/cwmsdb/schema/src/cwms/cwms_schema_pkg.sql
@/cwmsdb/schema/src/cwms/cwms_ts_pkg.sql
@/cwmsdb/schema/src/cwms/cwms_ts_pkg_body.sql
