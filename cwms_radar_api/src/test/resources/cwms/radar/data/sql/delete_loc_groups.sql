begin

    -- create a location group at SPK
    cwms_loc.delete_loc_group ('Test Category2',
                               'Test Group2',
                               'SPK'
        );
    cwms_loc.delete_loc_cat ('Test Category2',
                               'F',
                               'SPK'
        );

end;