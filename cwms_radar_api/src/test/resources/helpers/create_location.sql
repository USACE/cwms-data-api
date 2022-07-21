declare
    location_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(location_exists,-20026);
begin

  cwms_loc.create_location(
                ?, -- location id
                ?, --       p_location_type      IN VARCHAR2 DEFAULT NULL,
                ?, --       p_elevation          IN NUMBER DEFAULT NULL,
                ?, --          p_elev_unit_id       IN VARCHAR2 DEFAULT NULL,
                ?, --          p_vertical_datum      IN VARCHAR2 DEFAULT NULL,
                ?, --          p_latitude            IN NUMBER DEFAULT NULL,
                ?, --          p_longitude          IN NUMBER DEFAULT NULL,
                ?, --          p_horizontal_datum   IN VARCHAR2 DEFAULT NULL,
                ?, --          p_public_name         IN VARCHAR2 DEFAULT NULL,
                ?, --          p_long_name          IN VARCHAR2 DEFAULT NULL,
                ?, --          p_description         IN VARCHAR2 DEFAULT NULL,
                ?, --          p_time_zone_id       IN VARCHAR2 DEFAULT NULL,
                ?, --          p_county_name         IN VARCHAR2 DEFAULT NULL,
                ?, --          p_state_initial      IN VARCHAR2 DEFAULT NULL,
                ?, --          p_active             IN VARCHAR2 DEFAULT NULL,
                ? --          p_db_office_id       IN VARCHAR2 DEFAULT NULL
            );
    
exception
  when location_exists then null
    ;
end;