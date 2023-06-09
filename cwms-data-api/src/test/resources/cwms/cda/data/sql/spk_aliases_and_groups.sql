begin
    -- add a timeseries alias
    cwms_ts.store_ts_category('Test Category', 'For Testing', 'F', 'T', 'SPK');
    cwms_ts.store_ts_group('Test Category','Test Group','For testing','F','T',NULL,NULL,'SPK');
    cwms_ts.assign_ts_group(p_ts_category_id=>'Test Category',
                            p_ts_group_id=>'Test Group',
                            p_ts_id=>'Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda',
                            p_ts_attribute=>0,p_ts_alias_id=>'Alder Springs 15 Minute Rain Alias-cda',
                            p_ref_ts_id=>NULL,p_db_office_id=>'SPK');
end;