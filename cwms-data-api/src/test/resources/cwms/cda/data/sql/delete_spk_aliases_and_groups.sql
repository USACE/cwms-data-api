begin
    -- delete a timeseries alias
    cwms_ts.unassign_ts_group(p_ts_category_id=>'Test Category',
                                p_ts_group_id=>'Test Group',
                                p_ts_id=>'Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda',
                                p_unassign_all=>'T',
                                p_db_office_id=>'SPK');
    cwms_ts.delete_ts_group('Test Category','Test Group','SPK');
    cwms_ts.delete_ts_category('Test Category', 'T', 'SPK');
end;