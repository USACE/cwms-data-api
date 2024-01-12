declare
    P_TSID              varchar2(256)   := 'TsTextTestLoc.Flow.Inst.1Hour.0.raw';
    P_START_TIME   timestamp        := TO_TIMESTAMP_TZ('2005-02-01 13:30:00', 'YYYY-MM-DD HH24:MI:SS');
    P_END_TIME   timestamp          := TO_TIMESTAMP_TZ('2005-02-02 17:00:00', 'YYYY-MM-DD HH24:MI:SS');
    P_VERSION_DATE      date            := null;

begin
    CWMS_20.CWMS_TEXT.DELETE_TS_STD_TEXT(
        P_TSID => P_TSID,
        P_STD_TEXT_ID_MASK => '*',
        P_START_TIME => P_START_TIME,
        P_END_TIME => P_END_TIME,
        P_VERSION_DATE => P_VERSION_DATE,
        P_TIME_ZONE => 'UTC',
        P_MAX_VERSION => 'T',
        P_MIN_ATTRIBUTE => null,
        P_MAX_ATTRIBUTE => null,
        P_OFFICE_ID => 'SPK'
);

end;