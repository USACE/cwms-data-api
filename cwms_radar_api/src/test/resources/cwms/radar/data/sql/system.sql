begin
  FOR ofc in (select * from av_office where office_id not in ('CWMS','HQ','UNK'))
  LOOP
    cwms_msg.create_queues(ofc.office_id);
  END LOOP;
end;