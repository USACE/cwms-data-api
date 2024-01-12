begin
    CWMS_20.CWMS_TEXT.STORE_TS_STD_TEXT(
            P_TSID => 'TsTextTestLoc.Flow.Inst.1Hour.0.raw',
            P_STD_TEXT_ID => 'E',
--             P_START_TIME => TO_TIMESTAMP_TZ('2005-02-01 13:30:00-00:00','YYYY-MM-DD HH24:MI:SSTZH:TZM'),
--             P_END_TIME => TO_TIMESTAMP_TZ('2005-02-02 17:00:00-00:00','YYYY-MM-DD HH24:MI:SSTZH:TZM'),
            P_START_TIME => TO_TIMESTAMP_TZ('2005-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
            P_END_TIME => TO_TIMESTAMP_TZ('2005-02-03 17:00:00', 'YYYY-MM-DD HH24:MI:SS'),
            P_VERSION_DATE => null,
            P_TIME_ZONE => 'UTC',
            P_MAX_VERSION => 'T',
            P_EXISTING => 'T',
            P_NON_EXISTING => 'F',
            P_REPLACE_ALL => 'F',
            P_ATTRIBUTE => null,
            P_OFFICE_ID => 'SPK'
    );

end;