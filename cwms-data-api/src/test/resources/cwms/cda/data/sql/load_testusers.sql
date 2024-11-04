declare
    group_list cwms_20.char_32_array_type;
begin
    group_list := cwms_20.CHAR_32_ARRAY_TYPE('CWMS Users');
    cwms_20.cwms_sec.create_user('user','blah',group_list, 'HQ');
    cwms_20.cwms_sec.create_user('user2','blah',NULL,'SPK');
    cwms_20.cwms_sec.create_user('s0webtest','&password.',group_list,'HQ');
end;