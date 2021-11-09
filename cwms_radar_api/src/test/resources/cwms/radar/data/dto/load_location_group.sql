declare
    item_already_exists exception;
    pragma exception_init (item_already_exists, -20020);
    l_office         varchar2(16)            := 'LRL';
    l_category       varchar2(32)            := 'CWMS Mobile Location Listings';
    l_group          varchar2(32)            := 'Lakes';
    l_ts_cat         varchar2(32)            := 'CWMS Mobile ' || l_group;
    l_alias_type     cwms_20.loc_alias_type;
    l_ts_alias_tab_t cwms_20.ts_alias_tab_t;
    l_aliases        cwms_20.loc_alias_array := cwms_20.loc_alias_array(
            cwms_20.loc_alias_type('FakeLake', 'FakeLake'),
            cwms_20.loc_alias_type('NotThere', 'NotThere'),
            cwms_20.loc_alias_type('Testing', 'Testing')
        );

begin

        cwms_loc.create_location(
                'FakeLake',
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
                NULL, --          p_active             IN VARCHAR2 DEFAULT NULL,
                'LRL' --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
            );

        cwms_loc.create_location(
                'NotThere',
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
                NULL, --          p_active             IN VARCHAR2 DEFAULT NULL,
                'LRL' --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
            );
        cwms_loc.create_location(
                'Testing',
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
                NULL, --          p_active             IN VARCHAR2 DEFAULT NULL,
                'LRL' --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
            );

        cwms_ts.create_ts('LRL',
                         'FakeLake.Elev.Inst.1Hour.0.LRGS-rev',
                         NULL);
        cwms_ts.create_ts('LRL',
                          'FakeLake.Flow-Outflow.Ave.1Hour.1Hour.lrldlb-rev',
                          NULL);


        BEGIN
            DBMS_OUTPUT.put_line ('create loc group ' );

            cwms_loc.create_loc_group( p_loc_category_id => l_category, p_loc_group_id => l_group, p_loc_group_desc => NULL, p_db_office_id => l_office);
            DBMS_OUTPUT.put_line ('create ts cat ' );
            cwms_ts.store_ts_category( p_ts_category_id => l_ts_cat, p_ts_category_desc => NULL, p_db_office_id => l_office );
        EXCEPTION
            WHEN item_already_exists THEN
                NULL;
        END;

        BEGIN
            DBMS_OUTPUT.put_line ('unassign loc group ' );

            cwms_loc.unassign_loc_group( p_loc_category_id => l_category, p_loc_group_id => l_group, p_location_id => NULL, p_unassign_all => 'T', p_db_office_id => l_office);
            DBMS_OUTPUT.put_line ('assign loc groups ' );
            cwms_loc.assign_loc_groups( p_loc_category_id => l_category, p_loc_group_id => l_group, p_loc_alias_array => l_aliases, p_db_office_id => l_office);
            FOR elem IN 1 .. l_aliases.count
                LOOP
                    l_alias_type := l_aliases(elem);
                    DBMS_OUTPUT.put_line ('storing ts group ' || l_alias_type.loc_alias_id );
                    cwms_ts.store_ts_group( p_ts_category_id => l_ts_cat, p_ts_group_id => l_alias_type.loc_alias_id, p_ts_group_desc => NULL, p_fail_if_exists => 'F', p_ignore_nulls => 'T', p_shared_alias_id => NULL, p_shared_ts_ref_id => NULL, p_db_office_id => l_office);
                END LOOP;
        END;

    cwms_ts.unassign_ts_group(
            p_ts_category_id => l_ts_cat,
            p_ts_group_id => 'FakeLake',
            p_ts_id => null,
            p_unassign_all => 'T',
            p_db_office_id => l_office);
    l_ts_alias_tab_t := cwms_20.ts_alias_tab_t(
            cwms_20.ts_alias_t('FakeLake.Elev.Inst.1Hour.0.LRGS-rev', 1, 'Elev', null),
            cwms_20.ts_alias_t('FakeLake.Flow-Outflow.Ave.1Hour.1Hour.lrldlb-rev', 2, 'Outflow', null)
        );
    cwms_ts.assign_ts_groups(
            p_ts_category_id => l_ts_cat,
            p_ts_group_id => 'FakeLake',
            p_ts_alias_array => l_ts_alias_tab_t,
            p_db_office_id => l_office
        );


end;
