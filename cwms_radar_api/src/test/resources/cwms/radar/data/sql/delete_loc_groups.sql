begin

    -- create a location group at SPK
    cwms_loc.delete_loc_group (p_loc_category_id => 'Test Category2',
                               p_loc_group_id => 'Test Group2',
                               p_cascade      => 'F',
                               p_db_office_id => 'SPK'
        );
    cwms_loc.delete_loc_cat (p_loc_category_id=>'Test Category2',
                               p_cascade=>'F',
                               p_db_office_id=>'SPK'
        );

end;