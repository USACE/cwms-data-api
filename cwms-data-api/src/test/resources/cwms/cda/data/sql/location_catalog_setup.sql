declare
ts_code   NUMBER;
    l_office         varchar2(16)            := 'SPK';
    l_loc_cat       varchar2(32)            := 'ALIASED LOC Category';
begin
-- create some locations
    cwms_loc.create_location('Alder Springs Streamflow',
                             null,
                             6000,
                             'ft',
                             'NAVD88',
                             40.0,
                             -120.0,
                             'NAD83',
                             'Alder Springs Streamflow',
                             'Alder Springs Streamflow',
                             'Alder Springs Streamflow',
                             'PST',
                             'Lassen',
                             'CA',
                             'T',
                             l_office
    );

    cwms_loc.create_location('Pine Flat-Outflow Streamflow',
                             null,
                             4000,
                             'ft',
                             'NAVD88',
                             42.0,
                             -122.0,
                             'NAD83',
                             'Pine Flat-Outflow Streamflow',
                             'Pine Flat-Outflow Streamflow',
                             'Pine Flat-Outflow Streamflow',
                             'PST',
                             'Lassen',
                             'CA',
                             'T',
                             l_office
    );

    cwms_loc.store_loc_category(l_loc_cat,
                               'For Testing',
                               'F',
                               'T',
                               l_office
    );

    cwms_loc.store_loc_group (l_loc_cat,
                              'Group 1',
                              'For Testing',
                              'F',
                              'T',
                              'Alder Springs Streamflow',
                              null,
                              l_office
    );

    cwms_loc.store_loc_group (l_loc_cat,
                              'Group 2',
                              'For Testing',
                              'F',
                              'T',
                              null,
                              null,
                              l_office
    );

    cwms_loc.unassign_loc_group( p_loc_category_id => l_loc_cat, p_loc_group_id =>  'Group 1', p_location_id => NULL, p_unassign_all => 'T', p_db_office_id => l_office);
    cwms_loc.unassign_loc_group( p_loc_category_id =>l_loc_cat, p_loc_group_id => 'Group 2', p_location_id => NULL, p_unassign_all => 'T', p_db_office_id => l_office);

    cwms_loc.assign_loc_group(l_loc_cat,
                              'Group 1',
                              'Alder Springs Streamflow',
                              'Alder Stream Alias Loc',
                              l_office
    );
    cwms_loc.assign_loc_group(l_loc_cat,
                              'Group 2',
                              'Alder Springs Streamflow',
                              'Alder Stream Alias Loc 2',
                              l_office
    );
    cwms_loc.assign_loc_group(l_loc_cat,
                                'Group 2',
                                'Pine Flat-Outflow Streamflow',
                                'Pine Stream Alias Loc',
                                l_office
    );

end;