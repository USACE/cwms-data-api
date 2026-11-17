declare
  l_count number;
begin
  FOR ofc in (select * from av_office where office_id not in ('CWMS','HQ','UNK'))
  LOOP
    select count(*) into l_count from dba_queues where name=upper(ofc.office_id) || '_TS_STORED';
    if l_count = 0 then
      cwms_msg.create_queues(ofc.office_id);
    end if;
  END LOOP;

  -- togglz table for testing.
  execute immediate 'CREATE TABLE IF NOT EXISTS cwms_20.TOGGLZ (
                        FEATURE_NAME VARCHAR(100) PRIMARY KEY, FEATURE_ENABLED INTEGER, FEATURE_USERS VARCHAR(2000),
                        STRATEGY_ID VARCHAR(200), STRATEGY_PARAMS VARCHAR(2000)  
                      )';
  execute immediate 'create or replace public synonym TOGGLZ for cwms_20.TOGGLZ';
  execute immediate 'grant SELECT, INSERT, UPDATE, DELETE on cwms_20.TOGGLZ to web_user';

end;



