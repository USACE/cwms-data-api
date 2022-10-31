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
        p_ts_id=>'Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar',
        p_ts_attribute=>0,p_ts_alias_id=>'Alder Springs 15 Minute Rain Alias-radar',
        p_ref_ts_id=>NULL,p_db_office_id=>'SPK');

    --     create a location at LRL
    cwms_loc.create_location(
            'Clear Creek',
            NULL, --       p_location_type      IN VARCHAR2 DEFAULT NULL,
            NULL, --       p_elevation          IN NUMBER DEFAULT NULL,
            NULL, --          p_elev_unit_id       IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_vertical_datum      IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_latitude            IN NUMBER DEFAULT NULL,
            NULL, --          p_longitude          IN NUMBER DEFAULT NULL,
            NULL, --          p_horizontal_datum   IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_public_name         IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_long_name          IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_description         IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_time_zone_id       IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_county_name         IN VARCHAR2 DEFAULT NULL,
            NULL, --          p_state_initial      IN VARCHAR2 DEFAULT NULL,
            'T', --          p_active             IN VARCHAR2 DEFAULT NULL,
            'LRL' --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
        );

    cwms_ts.create_ts('LRL',
        'Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-radar',
        null);

    cwms_ts.assign_ts_group(
            p_ts_category_id=>'Test Category2',
            p_ts_group_id=>'Test Group2',
            p_ts_id=>'Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-radar',
            p_ts_attribute=>1,
            p_ts_alias_id=>'Clear Creek 15 Minute Rain Alias',
            p_ref_ts_id=>NULL,
            p_db_office_id=>'LRL');

end;