   set define on
define OFFICE_EROC=&1
defin API_KEY=&2
begin
   cwms_sec.add_user_to_group(
      '&&OFFICE_EROC.webtest',
      'All Users',
      'HQ'
   );
   cwms_sec.add_user_to_group(
      '&&OFFICE_EROC.webtest',
      'All Users',
      'SPK'
   );
   cwms_sec.add_user_to_group(
      '&&OFFICE_EROC.webtest',
      'CWMS Users',
      'HQ'
   );
   cwms_sec.add_user_to_group(
      '&&OFFICE_EROC.webtest',
      'CWMS User Admins',
      'HQ'
   );
   cwms_sec.add_cwms_user(
      'l2hectest',
      null,
      'SPK'
   );
   cwms_sec.update_edipi(
      'l2hectest',
      1234567890
   );
   cwms_sec.add_user_to_group(
      'l2hectest',
      'All Users',
      'SPK'
   );
   cwms_sec.add_user_to_group(
      'l2hectest',
      'CWMS Users',
      'SPK'
   );
   cwms_sec.add_user_to_group(
      'l2hectest',
      'TS ID Creator',
      'SPK'
   );
   cwms_sec.add_cwms_user(
      'l1hectest',
      null,
      'SPL'
   );
    -- intentionally no extra permissions.
    --cwms_sec.add_user_to_group('l2hectest','CWMS Users', 'SPL');


   cwms_sec.add_cwms_user(
      'm5hectest',
      null,
      'SWT'
   );
   cwms_sec.add_user_to_group(
      'm5hectest',
      'All Users',
      'SWT'
   );
   cwms_sec.add_user_to_group(
      'm5hectest',
      'CWMS Users',
      'SWT'
   );
   cwms_sec.add_cwms_user(
      'q0hectest',
      null,
      'SWT'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'All Users',
      'SWT'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'CWMS Users',
      'SWT'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'CWMS PD Users',
      'SWT'
   );
   cwms_sec.add_cwms_user(
      'q0hectest',
      null,
      'MVP'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'All Users',
      'MVP'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'CWMS Users',
      'MVP'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'CWMS PD Users',
      'MVP'
   );
   cwms_sec.add_cwms_user(
      'q0hectest',
      null,
      'LRL'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'All Users',
      'LRL'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'CWMS Users',
      'LRL'
   );
   cwms_sec.add_user_to_group(
      'q0hectest',
      'CWMS PD Users',
      'LRL'
   );
   execute immediate 'grant execute on cwms_20.cwms_upass to web_user';
   insert into "CWMS_20"."AT_API_KEYS" (
      userid,
      key_name,
      apikey,
      created,
      expires
   ) values ( 'Q0HECTEST',
              'test',
              '&&API_KEY',
              to_date('2025-06-10 16:10:42','YYYY-MM-DD HH24:MI:SS'),
              to_date('2029-06-16 16:10:46','YYYY-MM-DD HH24:MI:SS') );

    cwms_sec.add_cwms_user('m5hectest',NULL,'SWT');
    cwms_sec.add_user_to_group('m5hectest','All Users', 'SWT');
    cwms_sec.add_user_to_group('m5hectest','CWMS Users', 'SWT');
    execute immediate 'grant execute on cwms_20.cwms_upass to web_user';


    cwms_sec.add_cwms_user('m5testadmin', NULL, 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','All Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS User Admins', 'LRL');

end;
/
quit;