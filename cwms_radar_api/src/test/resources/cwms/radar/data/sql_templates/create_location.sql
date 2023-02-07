declare
    p_location varchar2(64) := ?;
    p_active char(1) := ?;
    p_office varchar2(10) := ?;
begin
cwms_loc.create_location(
                p_location,
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
                p_active, --          p_active             IN VARCHAR2 DEFAULT NULL,
                p_office --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
            );
end;