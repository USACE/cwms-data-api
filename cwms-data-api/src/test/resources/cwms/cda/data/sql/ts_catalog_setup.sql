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

    cwms_loc.create_location('Wet Meadows',
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
                             l_office
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