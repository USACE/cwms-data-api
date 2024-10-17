begin

    -- unassign a ts to the group
    cwms_ts.unassign_ts_group(
            p_ts_category_id=>'Test Category2',
            p_ts_group_id=>'Test Group2',
            p_ts_id=>'Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-cda',
            p_unassign_all=>'T',
            p_db_office_id=>'LRL');


    -- unassign a ts to the group
       cwms_ts.unassign_ts_group(
           p_ts_category_id=>'Test Category2',
           p_ts_group_id=>'Test Group2',
           p_ts_id=>'Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda',
           p_unassign_all=>'T',
           p_db_office_id=>'SPK');

    -- delete a group at CWMS in the mew category
    cwms_ts.delete_ts_group('Test Category2',
        'Test Group2',
        'CWMS');

    -- delete a category at CWMS
    cwms_ts.delete_ts_category('Test Category2',
        'T',
        'CWMS');






end;