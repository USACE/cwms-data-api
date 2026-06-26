   set define on
define OFFICE_EROC=&1
begin
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'All Users', 'HQ');
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'All Users', 'SPK');
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'CWMS Users', 'HQ');
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'CWMS User Admins', 'HQ');
   cwms_sec.add_cwms_user('l2hectest', null, 'SPK');
   cwms_sec.update_edipi('l2hectest', 1234567890);
   cwms_sec.add_user_to_group('l2hectest', 'All Users', 'SPK');
   cwms_sec.add_user_to_group('l2hectest', 'CWMS Users', 'SPK');
   cwms_sec.add_user_to_group('l2hectest', 'SHOW STACK TRACE', 'SPK');
   cwms_sec.add_user_to_group('l2hectest', 'TS ID Creator', 'SPK');
   cwms_sec.add_cwms_user('l1hectest', null, 'SPL');
    -- intentionally no extra permissions.
    --cwms_sec.add_user_to_group('l2hectest','CWMS Users', 'SPL');


   cwms_sec.add_cwms_user('m5hectest', null, 'SWT');
   cwms_sec.add_user_to_group('m5hectest', 'All Users', 'SWT');
   cwms_sec.add_user_to_group('m5hectest', 'CWMS Users', 'SWT');
   cwms_sec.add_user_to_group('m5hectest', 'TS ID Creator', 'SWT');
   cwms_sec.add_cwms_user('q0hectest', null, 'SWT');
   cwms_sec.add_user_to_group('q0hectest', 'All Users', 'SWT');
   cwms_sec.add_user_to_group('q0hectest', 'CWMS Users', 'SWT');
   cwms_sec.add_user_to_group('q0hectest', 'CWMS PD Users', 'SWT');
   cwms_sec.add_user_to_group('q0hectest', 'TS ID Creator', 'SWT');
   cwms_sec.add_cwms_user('q0hectest', null, 'MVP');
   cwms_sec.add_user_to_group('q0hectest', 'All Users', 'MVP');
   cwms_sec.add_user_to_group('q0hectest', 'CWMS Users', 'MVP');
   cwms_sec.add_user_to_group('q0hectest', 'CWMS PD Users', 'MVP');
   cwms_sec.add_user_to_group('q0hectest', 'TS ID Creator', 'MVP');
   cwms_sec.add_cwms_user('q0hectest', null, 'LRL');
   cwms_sec.add_user_to_group('q0hectest', 'All Users', 'LRL');
   cwms_sec.add_user_to_group('q0hectest', 'CWMS Users', 'LRL');
   cwms_sec.add_user_to_group('q0hectest', 'CWMS PD Users', 'LRL');
   cwms_sec.add_user_to_group('q0hectest', 'TS ID Creator', 'LRL');
   execute immediate 'grant execute on cwms_20.cwms_upass to web_user';

    cwms_sec.add_cwms_user('m5hectest',NULL,'SWT');
    cwms_sec.add_user_to_group('m5hectest','All Users', 'SWT');
    cwms_sec.add_user_to_group('m5hectest','CWMS Users', 'SWT');
    cwms_sec.add_user_to_group('m5hectest','TS ID Creator', 'SWT');
    execute immediate 'grant execute on cwms_20.cwms_upass to web_user';


    cwms_sec.add_cwms_user('m5testadmin', NULL, 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','All Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS Users', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','CWMS User Admins', 'LRL');
    cwms_sec.add_user_to_group('m5testadmin','SHOW STACK TRACE', 'LRL');

    begin
        insert into at_sec_cwms_users(userid, createdby, principle_name)
        values(
            'SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SWT',
            'CWMS_20',
            'http://localhost:8081/auth/realms/cwms::a4e88497-0ffc-41d5-b0fd-cc91760e366b'
        );
    exception
        when dup_val_on_index then
            update at_sec_cwms_users
            set principle_name = 'http://localhost:8081/auth/realms/cwms::a4e88497-0ffc-41d5-b0fd-cc91760e366b'
            where userid = 'SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SWT';
    end;
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SWT', 'All Users', 'SWT');
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SWT', 'CWMS Users', 'SWT');
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SWT', 'TS ID Creator', 'SWT');

    begin
        insert into at_sec_cwms_users(userid, createdby, principle_name)
        values(
            'SERVICE-ACCOUNT-CWMS-BATCH-AIRFLOW-SWT',
            'CWMS_20',
            'http://localhost:8081/auth/realms/cwms::b70c2e60-ce11-42c7-8271-10bb2b3fd4bd'
        );
    exception
        when dup_val_on_index then
            update at_sec_cwms_users
            set principle_name = 'http://localhost:8081/auth/realms/cwms::b70c2e60-ce11-42c7-8271-10bb2b3fd4bd'
            where userid = 'SERVICE-ACCOUNT-CWMS-BATCH-AIRFLOW-SWT';
    end;
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-AIRFLOW-SWT', 'All Users', 'SWT');
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-AIRFLOW-SWT', 'CWMS Users', 'SWT');

    begin
        insert into at_sec_cwms_users(userid, createdby, principle_name)
        values(
            'SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SPK',
            'CWMS_20',
            'http://localhost:8081/auth/realms/cwms::d2d6f91b-a5dd-40c3-8ee6-49a52da9892e'
        );
    exception
        when dup_val_on_index then
            update at_sec_cwms_users
            set principle_name = 'http://localhost:8081/auth/realms/cwms::d2d6f91b-a5dd-40c3-8ee6-49a52da9892e'
            where userid = 'SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SPK';
    end;
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SPK', 'All Users', 'SPK');
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SPK', 'CWMS Users', 'SPK');
    cwms_sec.add_user_to_group('SERVICE-ACCOUNT-CWMS-BATCH-RUNNER-SPK', 'TS ID Creator', 'SPK');

end;
/
quit;
