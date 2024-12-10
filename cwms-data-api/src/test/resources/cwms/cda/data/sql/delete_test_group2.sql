begin

    -- delete a group at CWMS in the mew category
    cwms_ts.delete_ts_group('Agency Aliases',
        'test_create_read_delete',
        'SWT');
end;