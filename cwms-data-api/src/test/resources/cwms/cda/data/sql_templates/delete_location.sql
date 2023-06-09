declare
    p_location varchar2(64) := ?;
    p_office varchar2(10) := ?;
begin
cwms_loc.delete_location(
        p_location_id   => p_location,
        p_delete_action => cwms_util.delete_all,
        p_db_office_id  => p_office);
end;