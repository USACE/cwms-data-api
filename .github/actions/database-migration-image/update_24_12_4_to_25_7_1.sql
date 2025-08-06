-------------------
-- general setup --
-------------------
--whenever sqlerror exit;
set define on
--set verify off
--set pages 100
--set serveroutput on
define cwms_schema = 'CWMS_20'
--define cwms_dba_schema = 'CWMS_DBA'
define cwms_dba_schema = 'DBAdmin'
alter session set current_schema = &cwms_schema;
set echo on;
------------------------------------------------------------
-- spool to file that identifies the database in the name --
------------------------------------------------------------
--var db_name varchar2(61)
--begin
--   select nvl(primary_db_unique_name, db_unique_name) into :db_name from v$database;
--end;
--/
--whenever sqlerror continue;
--declare
--   l_count pls_integer;
--   l_name  varchar2(30);
--begin
--   select count(*) into l_count from all_objects where object_name = 'CDB_PDBS';
--   if l_count > 0 then
--      select name
--        into l_name
--        from v$database;
--      :db_name := l_name;
--     begin
--         select pdb_name
--           into l_name
--           from cdb_pdbs;
--      exception
--         when no_data_found then
--            l_name := null;
--      end;
--      if l_name is not null then
--         :db_name := :db_name||'-'||l_name;
--      end if;
--   end if;
--end;
--/
--whenever sqlerror exit;
--column db_name new_value db_name
--select :db_name as db_name from dual;
--define logfile=update_&db_name._24_5_24_to_24_12_4.log
--PROMPT log file = &logfile
--spool &logfile append;
-------------------
-- do the update --
-------------------
--PROMPT ################################################################################
--PROMPT VERIFYING EXPECTED VERSION
--select systimestamp from dual;
--@@./25_07_01/verify_db_version

--PROMPT ################################################################################
--PROMPT SAVING PRE-UPDATE PRIVILEGES
--select systimestamp from dual;
--@@./util/preupdate_privs.sql;

PROMPT ################################################################################
PROMPT ALTERING TABLES
alter table at_api_keys modify userid varchar2(128);
alter table at_log_message modify session_username varchar2(128);
alter table at_log_message modify session_osuser varchar2(128);
alter table at_prj_lck_revoker_rights modify user_id varchar2(128);
alter table at_project_lock modify session_user varchar2(128);
alter table at_project_lock modify os_user varchar2(128);
alter table at_sec_cwms_users modify userid varchar2(128);
alter table at_sec_cwms_users modify createdby varchar2(128);
-- at_sec_cwms_users.principle_name takes more work since it's used in a virtual column
alter table at_sec_cwms_users drop column edipi;
alter table at_sec_cwms_users modify principle_name varchar2(512);
alter table at_sec_cwms_users add (edipi number(*,0) generated always as (case when length(principle_name)>10 then case  when  regexp_like (substr(principle_name,1,10),'^[[:digit:]]+$') then to_number(substr(principle_name,1,10)) else null end  else null end) virtual);alter table at_sec_locked_users modify username varchar2(128);
alter table at_sec_locked_users modify username varchar2(128);
alter table at_sec_service_user modify userid varchar2(128);
alter table at_sec_session modify userid varchar2(128);
alter table at_sec_users modify username varchar2(128);
alter table at_sec_user_office modify username varchar2(128);
alter table at_user_preferences modify username varchar2(128);
alter table temp_collection_api_fire_tbl modify user_id_fired varchar2(128);
alter table UPLOADED_XLS_FILES_T modify USER_ID_UPLOADED varchar2(128);
alter table UPLOADED_XLS_FILE_ROWS_T modify USER_ID_UPLOADED varchar2(128);
alter table UPLOADED_XLS_FILE_ROWS_T modify USER_ID_LAST_UPDATED varchar2(128);

PROMPT ################################################################################
PROMPT ALTERING VIEWS
@/cwmsdb/schema/src/cwms/
@/cwmsdb/schema/src/cwms/views/av_cwms_ts_id.sql
@/cwmsdb/schema/src/cwms/views/av_location_level.sql

PROMPT ################################################################################
PROMPT adding Data acquisition groups
@/schema/src/updateScripts/25_07_01/data_acquisition

PROMPT ################################################################################
PROMPT CREATING AND ALTERING TYPE SPECIFICATIONS
select systimestamp from dual;

drop type ztsv_type force;
@/cwmsdb/schema/src/cwms/types/ztsv_type
@/cwmsdb/schema/src/cwms/types/ztsv_entry_type
@/cwmsdb/schema/src/cwms/types/ztsv_entry_array
create or replace public synonym cwms_t_ztsv_entry for ztsv_entry_type;

PROMPT ################################################################################
PROMPT CREATING AND ALTERING TYPE BODIES
select systimestamp from dual;

@/cwmsdb/schema/src/cwms/types/loc_lvl_indicator_cond_t-body
@/cwmsdb/schema/src/cwms/types/rating_t-body
@/cwmsdb/schema/src/cwms/types/streamflow_meas_t-body
@/cwmsdb/schema/src/cwms/types/streamflow_meas2_t-body

PROMPT ################################################################################
PROMPT UPDATING PACKAGE SPECIFICATIONS

@/cwmsdb/schema/src/cwms/cwms_cache_pkg
@/cwmsdb/schema/src/cwms/cwms_sec_pkg
@/cwmsdb/schema/src/cwms/cwms_ts_pkg
@/cwmsdb/schema/src/cwms/cwms_util_pkg

PROMPT ################################################################################
PROMPT UPDATING PACKAGE BODIES
select systimestamp from dual;

define builduser = BUILDUSER

@/cwmsdb/schema/src/cwms/cwms_cache_pkg_body
@/cwmsdb/schema/src/cwms/cwms_display_pkg_body
@/cwmsdb/schema/src/cwms/cwms_env_pkg_body
@/cwmsdb/schema/src/cwms/cwms_lock_pkg_body
@/cwmsdb/schema/src/cwms/cwms_msg_pkg_body
@/cwmsdb/schema/src/cwms/cwms_priv_pkg_body
@/cwmsdb/schema/src/cwms/cwms_project_pkg_body
@/cwmsdb/schema/src/cwms/cwms_sec_pkg_body
@/cwmsdb/schema/src/cwms/cwms_rating_pkg_body
@/cwmsdb/schema/src/cwms/cwms_shef_pkg_body
@/cwmsdb/schema/src/cwms/cwms_ts_pkg_body
@/cwmsdb/schema/src/cwms/cwms_util_pkg_body

PROMPT ################################################################################
PROMPT UPDATING TRIGGERS
select systimestamp from dual;

create or replace function get_source(p_type varchar2, p_name varchar2) return clob
is
   l_clob clob;
begin
   dbms_lob.createtemporary(l_clob, true);
   dbms_lob.open(l_clob, dbms_lob.lob_readwrite);
   l_clob := '';
   for rec in (select text from user_source where type=upper(p_type) and name=upper(p_name) order by line) loop
      l_clob := l_clob || rec.text;
   end loop;
   return l_clob;
end;
/

declare
   l_sql clob;
begin
   for rec in (select object_name
               from all_objects
               where owner = 'CWMS_20'
                 and object_type = 'TRIGGER'
                 and object_name like 'ST\_%' escape '\' order by 1)
   loop
      begin
         l_sql := 'CREATE OR REPLACE '
            ||replace(get_source('TRIGGER', rec.object_name),
               'user NOT IN (''SYS'', ''CWMS_20'')', 
               'user NOT IN (''SYS'', ''CWMS_20'', upper(''builduser''))');
         l_sql := replace(l_sql,
               'USER NOT IN (''SYS'', ''CWMS_20'')',
               'USER NOT IN (''SYS'', ''CWMS_20'', upper(''builduser''))');
         execute immediate l_sql;
      end;
   end loop;
end;
/
drop function get_source;

PROMPT ################################################################################
PROMPT GRANT SELECT ON TABLES TO USERS
select systimestamp from dual;

begin
   for rec in (select object_name
                 from all_objects
                where owner = 'CWMS_20'
                  and object_type = 'TABLE'
                  and (object_name like 'AT\_%' escape '\' or object_name like 'CWMS\_%' escape '\')
                  and object_name not like 'AT_SEC_%'
                  and object_name != 'AT_API_KEYS'
              )
   loop
      dbms_output.put_line(rec.object_name);
      execute immediate 'grant select on '||rec.object_name||' to cwms_user';
   end loop;
end;
/

--PROMPT ################################################################################
--PROMPT FINAL HOUSEKEEPING
--select systimestamp from dual;

--declare
--   cmd varchar2(128);
--begin
--   execute immediate 'grant CWMS_USER to CWMS_DBA';
--   for rec in (select object_name,
--                      object_type
--                 from dba_objects
--                where owner = '&cwms_schema'
--                  and object_type in ('PACKAGE BODY', 'TYPE', 'VIEW')
--                  and object_name not like '%AQ$%'
--              )
--   loop
--      cmd := 'grant '
--         ||case when rec.object_type = 'VIEW' then 'select' else 'execute' end
--         ||' on &cwms_schema..'
--         ||rec.object_name
--         ||' to CWMS_USER';
--      dbms_output.put(cmd||' [');
--      begin
--         execute immediate cmd;
--         dbms_output.put_line('SUCCEEDED]');
--      exception
--         when others then dbms_output.put_line('FAILED]');
--      end;   
--   end loop;
--end;
--/
--PROMPT ################################################################################
--PROMPT RESTORING PRE-UPDATE PRIVILEGES
--@@./util/restore_privs

PROMPT ################################################################################
PROMPT RECOMPILING SCHEMA
select systimestamp from dual;
@/cwmsdb/schema/src/updateScripts/util/compile_objects

--promp ################################################################################
--PROMPT REMAINING INVALID OBJECTS...
--select systimestamp from dual;
--select owner||'.'||substr(object_name, 1, 30) as invalid_object,
--       object_type
--  from all_objects
-- where status = 'INVALID'
--   and owner in ('&cwms_schema', '&cwms_dba_schema')
-- order by 1, 2;
--select owner||'.'||substr(name, 1, 30) as name,
--       type,
--       substr(line||':'||position, 1, 12) as location,
--       substr(text, 1, 132) as error
--  from all_errors
-- where attribute = 'ERROR'
--   and owner in ('&cwms_schema', '&cwms_dba_schema')
-- order by owner, type, name, sequence;
--/

--whenever sqlerror exit;

PROMPT ################################################################################
PROMPT UPDATING DB_CHANGE_LOG
select systimestamp from dual;
@/cwmsdb/schema/src/updateScripts/25_07_01/update_db_change_log
--select substr(version, 1, 10) as version,
--       to_char(version_date, 'yyyy-mm-dd hh24:mi') as version_date,
--       to_char(apply_date, 'yyyy-mm-dd hh24:mi') as apply_date
--  from av_db_change_log
-- where application = 'CWMS'
-- order by version_date;
--declare
--   l_count pls_integer;
--begin
--   select count(*)
--     into l_count
--     from all_objects
--    where status = 'INVALID'
--      and owner in ('&cwms_schema', '&cwms_dba_schema');

--   if l_count > 0 then
--      raise_application_error(-20999, chr(10)||'==>'||chr(10)||'==> SOME OBJECTS ARE STILL INVALID'||chr(10)||'==>');
--   end if;
--end;
--/
--PROMPT ################################################################################
--PROMPT UPDATE COMPLETE
--select systimestamp from dual;
--exit
