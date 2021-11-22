begin
    cwms_loc.create_location(
        'Alder Springs',
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
        'SPK' --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
    );
    cwms_loc.create_location(
        'Wet Meadows',
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
        'SPK' --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
    );

    cwms_loc.create_location(
                'Pine Flat-Outflow',
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
                'SPK' --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
            );


    cwms_ts.create_ts('SPK','Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw');
    cwms_ts.create_ts('SPK','Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc');
    cwms_ts.create_ts('SPK','Pine Flat-Outflow.Stage.Inst.15Minutes.0.0.raw');
    cwms_ts.create_ts('SPK','Wet Meadows.Depth-SWE.Inst.15Minutes.0.0.raw');

    -- add a timeseries alias
    cwms_ts.store_ts_category('Test Category', 'For Testing', 'F', 'T', 'SPK');
    cwms_ts.store_ts_group('Test Category','Test Group','For testing','F','T',NULL,NULL,'SPK');
    cwms_ts.assign_ts_group(p_ts_category_id=>'Test Category',  p_ts_group_id=>'Test Group', p_ts_id=>'Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw', p_ts_attribute=>0,p_ts_alias_id=>'Alder Springs 15 Minute Rain Alias',p_ref_ts_id=>NULL,p_db_office_id=>'SPK');

end;