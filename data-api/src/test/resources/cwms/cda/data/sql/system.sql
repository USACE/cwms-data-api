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
end;