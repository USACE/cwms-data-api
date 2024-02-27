declare
    START_TIME   timestamp        := TO_TIMESTAMP_TZ('2005-01-01 0:30:00', 'YYYY-MM-DD HH24:MI:SS');
    END_TIME   timestamp          := TO_TIMESTAMP_TZ('2005-01-02 23:00:00', 'YYYY-MM-DD HH24:MI:SS');
begin
        CWMS_20.CWMS_TEXT.DELETE_TS_TEXT(
            P_TSID => 'TsTextTestLoc.Flow.Inst.1Hour.0.raw',
            P_TEXT_MASK => '*',
            P_START_TIME => START_TIME,
            P_END_TIME => END_TIME,
            P_VERSION_DATE => null,
            P_TIME_ZONE => 'UTC',
            P_MAX_VERSION => 'T',
            P_MIN_ATTRIBUTE => null,
            P_MAX_ATTRIBUTE => null,
            P_OFFICE_ID => 'SPK'
    );

--     call it again in-case two versions were stored in tests.
    CWMS_20.CWMS_TEXT.DELETE_TS_TEXT(
                P_TSID => 'TsTextTestLoc.Flow.Inst.1Hour.0.raw',
                P_TEXT_MASK => '*',
                P_START_TIME => START_TIME,
                P_END_TIME => END_TIME,
                P_VERSION_DATE => null,
                P_TIME_ZONE => 'UTC',
                P_MAX_VERSION => 'T',
                P_MIN_ATTRIBUTE => null,
                P_MAX_ATTRIBUTE => null,
                P_OFFICE_ID => 'SPK'
        );
end;