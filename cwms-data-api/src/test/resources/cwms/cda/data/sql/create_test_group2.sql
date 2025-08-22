begin

    -- create a group at CWMS in the mew category
    cwms_ts.store_ts_group('Agency Aliases',
        'test_create_read_delete',
        'IntegrationTesting','F','T',
        'sharedTsAliasId',
        'Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda',
        'SWT');

end;