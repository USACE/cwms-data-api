select samples.db_office_id,samples.cwms_ts_id,samples.ts_active_flag, 
       loc.location_id, loc.location_type,
       loc.elevation, loc.unit_id, loc.vertical_datum, loc.latitude, loc.longitude,
       loc.horizontal_datum, loc.public_name, loc.long_name, loc.description,
       loc.time_zone_name, loc.county_name, loc.state_initial
from
 (
    select *
    from
        (
        select 
            * 
        from 
            cwms_20.av_cwms_ts_id2 
        where db_office_id = ? and aliased_item is NULL
        order by dbms_random.value
    )
    where rownum <= 20
) samples
left outer join cwms_20.av_loc loc on loc.location_id = samples.location_id
order by samples.cwms_ts_id
