begin
  cwms_sec.add_user_to_group('&user.','CWMS Users', 'HQ');

  /** Add a couple of districts*/
  cwms_sec.add_cwms_user('l2hectest',NULL,'SPK');
  cwms_sec.add_user_to_group('l2hectest','CWMS Users','SPK');
  insert into at_api_keys(userid,key_name,apikey) values('L2HECTEST','l2 test key','l2userkey');
end;
