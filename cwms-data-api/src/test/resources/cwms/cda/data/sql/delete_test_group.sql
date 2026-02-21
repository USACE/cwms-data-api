begin
    -- delete a group at CWMS in the mew category
    cwms_ts.delete_ts_group(P_TS_CATEGORY_ID => 'TestCategory2',
        P_TS_GROUP_ID => 'test_create_read_delete',
        P_DB_OFFICE_ID => 'SWT');
end;