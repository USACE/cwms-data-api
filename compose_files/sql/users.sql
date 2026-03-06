   set define on
define OFFICE_EROC=&1
defin API_KEY=&2
begin
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'All Users', 'HQ');
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'All Users', 'SPK');
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'CWMS Users', 'HQ');
   cwms_sec.add_user_to_group('&&OFFICE_EROC.webtest', 'CWMS User Admins', 'HQ');
   cwms_sec.add_cwms_user('l2hectest', null, 'SPK');
   cwms_sec.update_edipi('l2hectest', 1234567890);
   cwms_sec.add_user_to_group('l2hectest', 'All Users', 'SPK');
   cwms_sec.add_user_to_group('l2hectest', 'CWMS Users', 'SPK');
   cwms_sec.add_user_to_group('l2hectest', 'TS ID Creator', 'SPK');
   cwms_sec.add_cwms_user('l1hectest', null, 'SPL');
    cwms_sec.add_user_to_group('l1hectest','All Users', 'SPL');
    cwms_sec.add_user_to_group('l1hectest','CWMS Users', 'SPL');
    -- Viewer Users role assigned later in persona section


   cwms_sec.add_cwms_user('m5hectest', null, 'SWT');
   cwms_sec.add_user_to_group('m5hectest', 'All Users', 'SWT');
   cwms_sec.add_user_to_group('m5hectest', 'CWMS Users', 'SWT');
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

    -- Create persona user groups for authorization testing
    -- Note: cwms_sec.create_user_group auto-assigns user_group_code sequentially.
    -- To achieve the desired priority order (12-16), create in this specific order:
    -- 1. data_manager (code 12 - highest priority, embargo exempt)
    -- 2. water_manager (code 13 - embargo exempt)
    -- 3. dam_operator (code 14 - subject to embargo)
    -- 4. external_cooperator (code 15 - subject to embargo)
    -- Viewer Users is a built-in CWMS role and should be at code 16.
    -- See docs/ts-group-authorization-reference.md for full priority table.
    BEGIN cwms_sec.create_user_group('data_manager', 'Data management personnel - embargo exempt', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_user_group('water_manager', 'Water management personnel - embargo exempt', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_user_group('dam_operator', 'Dam operations personnel - subject to embargo rules', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_user_group('external_cooperator', 'External partners - subject to embargo', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;

    -- Assign test users to persona roles
    BEGIN cwms_sec.add_user_to_group('m5hectest', 'dam_operator', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.add_user_to_group('l2hectest', 'water_manager', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.add_user_to_group('l1hectest', 'Viewer Users', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;

    -- Create TS groups with policy naming convention: policy-<role>-<action>-<time>
    BEGIN cwms_sec.create_ts_group('policy-dam_operator-r-72h', 'Dam operators read access - 72 hour embargo', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_ts_group('policy-dam_operator-r-7d', 'Dam operators read access - 7 day embargo', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_ts_group('policy-water_manager-rw-0h', 'Water managers read-write - no embargo', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_ts_group('policy-data_manager-rw-0h', 'Data managers read-write - no embargo', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_ts_group('policy-viewer_users-r-7d', 'Viewer users read access - 7 day embargo', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.create_ts_group('policy-external_cooperator-r-4d', 'External cooperators read access - 4 day embargo', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;

    -- Assign TS groups to persona user groups
    BEGIN cwms_sec.assign_ts_group_user_group('policy-dam_operator-r-72h', 'dam_operator', 'Read', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.assign_ts_group_user_group('policy-dam_operator-r-7d', 'dam_operator', 'Read', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.assign_ts_group_user_group('policy-water_manager-rw-0h', 'water_manager', 'Read-Write', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.assign_ts_group_user_group('policy-data_manager-rw-0h', 'data_manager', 'Read-Write', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.assign_ts_group_user_group('policy-viewer_users-r-7d', 'Viewer Users', 'Read', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
    BEGIN cwms_sec.assign_ts_group_user_group('policy-external_cooperator-r-4d', 'external_cooperator', 'Read', 'HQ');
    EXCEPTION WHEN OTHERS THEN NULL; END;
end;
/
quit;