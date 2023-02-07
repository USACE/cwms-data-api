begin
  --cwms_sec.add_user_to_group('&user.','CWMS Users', 'HQ');
  
  cwms_sec.add_user_to_group('s0webtest','CWMS Users', 'HQ');
  cwms_sec.add_user_to_group('s0webtest','CWMS PD Users', 'HQ');
  cwms_sec.add_user_to_group('s0webtest','CWMS DBA Users', 'HQ');
  cwms_sec.add_user_to_group('s0webtest','CWMS Users', 'SPK');
  cwms_sec.add_user_to_group('s0webtest','CWMS PD Users', 'SPK');
  cwms_sec.add_user_to_group('s0webtest','CWMS DBA Users', 'SPK');
  

  /** Add a couple of districts*/
  begin
    cwms_sec.add_cwms_user('l2hectest',NULL,'SPK');
    cwms_sec.add_user_to_group('l2hectest','CWMS Users','SPK');
    cwms_sec.add_user_to_group('l2hectest','TS ID Creator','SPK');
    insert into at_api_keys(userid,key_name,apikey) values('L2HECTEST','l2 test key','l2userkey');
  exception
    when dup_val_on_index then null; -- user already exists
  end;

  begin
    cwms_sec.add_cwms_user('user2',NULL,'SPK');
    insert into at_api_keys(userid,key_name,apikey) values('USER2','USER test key','User2key');
  exception
    when dup_val_on_index then null; -- user already exists
  end;
end;
