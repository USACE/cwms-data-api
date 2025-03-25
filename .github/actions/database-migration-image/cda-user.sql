set define on
@/tmp/cda-user-info
grant web_user to &&cda_user;
begin
    -- create user
    cwms_sec.add_cwms_user('&&cda_user', NULL, 'HQ');
    -- set permissions for all offices
    for ofc in (select office_id from av_office where office_id not in ('CWMS', 'UNK'))
    loop
        cwms_sec.add_user_to_group('&&cda_user','All Users', ofc.office_id);
        cwms_sec.add_user_to_group('&&cda_user','CWMS Users', ofc.office_id);
        cwms_sec.add_user_to_group('&&cda_user','CWMS DBA Users', ofc.office_id);
        cwms_sec.add_user_to_group('&&cda_user','CWMS User Admins', ofc.office_id);
        cwms_sec.add_user_to_group('&&cda_user','CWMS PD Users', ofc.office_id);
    end loop;
end;
/
