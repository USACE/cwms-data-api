begin

    -- create a category at CWMS
    cwms_ts.store_ts_category('Test Category2',
        'Category For Testing',
        'F', 'T',
        'CWMS');

    -- create a group at CWMS in the mew category
    cwms_ts.store_ts_group('Test Category2',
        'Test Group2',
        'Group For testing','F','T',NULL,NULL,
        'CWMS');

    -- assign a ts to the group
    cwms_ts.assign_ts_group(
        p_ts_category_id=>'Test Category2',
        p_ts_group_id=>'Test Group2',
        p_ts_id=>'Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda',
        p_ts_attribute=>0,p_ts_alias_id=>'Alder Springs 15 Minute Rain Alias-cda',
        p_ref_ts_id=>NULL,p_db_office_id=>'SPK');
    

    cwms_ts.assign_ts_group(
            p_ts_category_id=>'Test Category2',
            p_ts_group_id=>'Test Group2',
            p_ts_id=>'Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-cda',
            p_ts_attribute=>1,
            p_ts_alias_id=>'Clear Creek 15 Minute Rain Alias',
            p_ref_ts_id=>NULL,
            p_db_office_id=>'LRL');

end;