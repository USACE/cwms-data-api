declare
    ts_code   NUMBER;
    l_office         varchar2(16)            := 'SPK';
    l_loc_cat       varchar2(32)            := 'Test Category';
    l_ts_cat         varchar2(32)            := 'Test Category';
begin
    -- to test the ts catalog query we need some complicated timeseries groups and location groups
    -- this is so users can use the ts group like and location group like catalog filters

    -- create some locations
    cwms_loc.create_location('Alder Springs',
                             null,
                             5000,
                             'ft',
                             'NAVD88',
                             40.0,
                             -120.0,
                             'NAD83',
                             'Alder Springs',
                             'Alder Springs',
                             'Alder Springs',
                             'PST',
                             'Lassen',
                             'CA',
                             'T',
                             l_office
    );

    cwms_loc.create_location('Pine Flat-Outflow',
                             null,
                             3000,
                             'ft',
                             'NAVD88',
                             42.0,
                             -122.0,
                             'NAD83',
                             'Pine Flat-Outflow',
                             'Pine Flat-Outflow',
                             'Pine Flat-Outflow',
                             'PST',
                             'Lassen',
                             'CA',
                             'T',
                             l_office
    );

    -- need to call store_location2 to set the bounding office id
    /**
     * Stores (inserts or updates) a location in the database
     *
     * @param p_location_id         The location identifier
     * @param p_location_type       A user-defined type for the location
     * @param p_elevation           The elevation of the location
     * @param p_elev_unit_id        The elevation unit
     * @param p_vertical_datum      The datum of the elevation
     * @param p_latitude            The actual latitude of the location
     * @param p_longitude           The actual longitude of the location
     * @param p_horizontal_datum    The datum for the latitude and longitude
     * @param p_public_name         The public name for the location
     * @param p_long_name           The long name for the location
     * @param p_description         A description of the location
     * @param p_time_zone_id        The time zone name for the location
     * @param p_county_name         The name of the county that the location is in
     * @param p_state_initial       The two letter abbreviation of the state that the location is in
     * @param p_active              A flag ('T' or 'F') that specifies whether the location is marked as active
     * @param p_location_kind_id    THIS PARAMETER IS IGNORED. A site created with this procedure will have a location kind of SITE.
     * @param p_map_label           A label to be used on maps for location
     * @param p_published_latitude  The published latitude for the location
     * @param p_published_longitude The published longitude for the location
     * @param p_bounding_office_id  The office whose boundary encompasses the location
     * @param p_nation_id           The nation that the location is in
     * @param p_nearest_city        The name of the city nearest to the location
     * @param p_ignorenulls         A flag ('T' or 'F') that specifies whether to ignore NULL parameters. If 'F', existing data will be updated with NULL parameter values.
     * @param p_db_office_id        The office that owns the location. If not specified or NULL, the session user's default office will be used
     */
--     PROCEDURE store_location2 (
    cwms_loc.store_location2('Wet Meadows',
                             null,
                             1000,
                             'ft',
                             'NAVD88',
                             41.0,
                             -121.0,
                             'NAD83',
                             'Wet Meadows',
                             'Wet Meadows',
                             'Wet Meadows',
                             'PST',
                             'Lassen',
                             'CA',
                             'T',
                             l_office,
                              'map label goes here',
                              41.0,
                              -121.0,
        'SPK',
        null,
        null,
        'F',
    'SPK'
    );

    -- create some timeseries
    cwms_ts.create_ts_code(ts_code, 'Pine Flat-Outflow.Stage.Inst.15Minutes.0.one', null,null,null, null, 'T','F', l_office);
    cwms_ts.create_ts_code(ts_code, 'Pine Flat-Outflow.Stage.Inst.15Minutes.0.two', null,null,null, null, 'T','F', l_office);
    cwms_ts.create_ts_code(ts_code, 'Pine Flat-Outflow.Stage.Inst.15Minutes.0.three', null,null,null, null, 'T','F', l_office);
    cwms_ts.create_ts_code(ts_code, 'Pine Flat-Outflow.Stage.Inst.15Minutes.0.four', null,null,null, null, 'T','F', l_office);
    cwms_ts.create_ts_code(ts_code, 'Wet Meadows.Depth-SWE.Inst.15Minutes.0.one', null,null,null, null, 'T','F', l_office);
    cwms_ts.create_ts_code(ts_code, 'Wet Meadows.Depth-SWE.Inst.15Minutes.0.two', null,null,null, null, 'T','F', l_office);
    cwms_ts.create_ts_code(ts_code, 'Wet Meadows.Depth-SWE.Inst.15Minutes.0.three', null,null,null, null, 'T','F', l_office);
    cwms_ts.create_ts_code(ts_code, 'Wet Meadows.Depth-SWE.Inst.15Minutes.0.four', null,null,null, null, 'T','F', l_office);


    -- two timeseries groups.
    cwms_ts.store_ts_category(l_ts_cat, 'For Testing', 'F', 'T', l_office);
    cwms_ts.store_ts_group(l_ts_cat,'Evens','For testing','F','T',NULL,NULL,l_office);
    cwms_ts.store_ts_group(l_ts_cat,'LessThan3','For testing','F','T',NULL,NULL,l_office);

    cwms_ts.unassign_ts_group(
            p_ts_category_id => l_ts_cat,
            p_ts_group_id => 'Evens',
            p_ts_id => null,
            p_unassign_all => 'T',
            p_db_office_id => l_office);

    cwms_ts.unassign_ts_group(
            p_ts_category_id => l_ts_cat,
            p_ts_group_id => 'LessThan3',
            p_ts_id => null,
            p_unassign_all => 'T',
            p_db_office_id => l_office);

    -- Assign TS
    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'LessThan3',
                            p_ts_id=>'Pine Flat-Outflow.Stage.Inst.15Minutes.0.one',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'LessThan3',
                            p_ts_id=>'Pine Flat-Outflow.Stage.Inst.15Minutes.0.two',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'Evens',
                            p_ts_id=>'Pine Flat-Outflow.Stage.Inst.15Minutes.0.two',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'Evens',
                            p_ts_id=>'Pine Flat-Outflow.Stage.Inst.15Minutes.0.four',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    -- Assign Wet Meadows TS
    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'LessThan3',
                            p_ts_id=>'Wet Meadows.Depth-SWE.Inst.15Minutes.0.one',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'LessThan3',
                            p_ts_id=>'Wet Meadows.Depth-SWE.Inst.15Minutes.0.two',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'Evens',
                            p_ts_id=>'Wet Meadows.Depth-SWE.Inst.15Minutes.0.two',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    cwms_ts.assign_ts_group(p_ts_category_id=>l_ts_cat,
                            p_ts_group_id=>'Evens',
                            p_ts_id=>'Wet Meadows.Depth-SWE.Inst.15Minutes.0.four',
                            p_ts_attribute=>0,
                            p_ts_alias_id=>null,
                            p_ref_ts_id=>NULL,
                            p_db_office_id=>l_office);

    -- Also need some location groups at SPK
    cwms_loc.store_loc_group (l_loc_cat,
                              'A to M',
                              'For Testing',
                              'F',
                              'T',
                              null,
                              null,
                              l_office
    );

    cwms_loc.store_loc_group (l_loc_cat,
                              'N to Z',
                              'For Testing',
                              'F',
                              'T',
                              null,
                              null,
                              l_office
    );

    cwms_loc.unassign_loc_group( p_loc_category_id => l_loc_cat, p_loc_group_id =>  'A to M', p_location_id => NULL, p_unassign_all => 'T', p_db_office_id => l_office);
    cwms_loc.unassign_loc_group( p_loc_category_id =>l_loc_cat, p_loc_group_id => 'N to Z', p_location_id => NULL, p_unassign_all => 'T', p_db_office_id => l_office);

    -- the p_loc_alias_id (2nd to last param, just before office) have to be set to something(anything non-null) for the extra rows with
    -- LOC_ALIAS_GROUP to show up in AV_CWMS_TS_ID2. I don't think we want to count on those so not setting them here.
    cwms_loc.assign_loc_group(l_loc_cat,
                              'A to M',
                              'Alder Springs',
                              null,
                              l_office
    );
    cwms_loc.assign_loc_group(l_loc_cat,
                                'N to Z',
                                'Pine Flat-Outflow',
                                null,
                                l_office
        );

    cwms_loc.assign_loc_group(l_loc_cat,
                              'N to Z',
                              'Wet Meadows',
                              null,
                              l_office
    );

end;