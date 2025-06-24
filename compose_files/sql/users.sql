set define on
define OFFICE_EROC=&1
begin
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'HQ');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','All Users', 'SPK');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','CWMS Users', 'HQ');
    cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest','CWMS User Admins', 'HQ');
    

    cwms_sec.add_cwms_user('l2hectest',NULL,'SPK');
    cwms_sec.update_edipi('l2hectest',1234567890);
    cwms_sec.add_user_to_group('l2hectest','All Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest','CWMS Users', 'SPK');
    cwms_sec.add_user_to_group('l2hectest','TS ID Creator','SPK');

    cwms_sec.add_cwms_user('l1hectest',NULL,'SPL');
    -- intentionally no extra permissions.
    --cwms_sec.add_user_to_group('l2hectest','CWMS Users', 'SPL');

    cwms_sec.add_cwms_user('m5hectest',NULL,'SWT');
    cwms_sec.add_user_to_group('m5hectest','All Users', 'SWT');
    cwms_sec.add_user_to_group('m5hectest','CWMS Users', 'SWT');
    execute immediate 'grant execute on cwms_20.cwms_upass to web_user';


    cwms_sec.add_user_cwms('m5testadmin', NULL, 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','All Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS User Admins', 'LRL');
end;
/
quit;