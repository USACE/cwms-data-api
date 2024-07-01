begin

    -- create a location group at SPK
    cwms_loc.store_loc_group ('Test Category2',
                     'Test Group2',
                     'For Testing',
                     'F',
                     'T',
                     null,
                     null,
                     'SPK'
        );

    cwms_loc.store_loc_group ('Test Category3',
                              'Test Group3',
                              'For Testing',
                              'F',
                              'T',
                              null,
                              null,
                              'SPK'
        );



end;