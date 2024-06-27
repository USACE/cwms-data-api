declare
    l_office         varchar2(16)            := 'SPK';
    l_loc_cat       varchar2(32)            := 'Test Category';
    l_ts_cat         varchar2(32)            := 'Test Category';
begin


    cwms_ts.unassign_ts_group(
            p_ts_category_id => l_ts_cat,
            p_ts_group_id => 'Evens',
            p_ts_id => null,
            p_unassign_all => 'T',
            p_db_office_id => l_office);
    cwms_ts.delete_ts_group (p_ts_category_id  => l_ts_cat,
                             p_ts_group_id     => 'Evens',
                             p_db_office_id    => l_office);


    cwms_ts.unassign_ts_group(
            p_ts_category_id => l_ts_cat,
            p_ts_group_id => 'LessThan3',
            p_ts_id => null,
            p_unassign_all => 'T',
            p_db_office_id => l_office);
    cwms_ts.delete_ts_group (p_ts_category_id  => l_ts_cat,
                             p_ts_group_id     => 'LessThan3',
                             p_db_office_id    => l_office);

    cwms_loc.unassign_loc_group( p_loc_category_id => l_loc_cat, p_loc_group_id =>  'A to M', p_location_id => NULL, p_unassign_all => 'T', p_db_office_id => l_office);
    cwms_loc.delete_loc_group (p_loc_category_id => l_loc_cat,
                               p_loc_group_id => 'A to M',
                               p_cascade      => 'F',
                               p_db_office_id => l_office
    );

    cwms_loc.unassign_loc_group( p_loc_category_id =>l_loc_cat, p_loc_group_id => 'N to Z', p_location_id => NULL, p_unassign_all => 'T', p_db_office_id => l_office);
    cwms_loc.delete_loc_group (p_loc_category_id => l_loc_cat,
                               p_loc_group_id => 'N to Z',
                               p_cascade      => 'F',
                               p_db_office_id => l_office
    );


end;