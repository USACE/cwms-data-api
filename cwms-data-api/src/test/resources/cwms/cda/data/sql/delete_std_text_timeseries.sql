declare
    P_TSID              varchar2(256)   := 'First519402.Flow.Inst.1Hour.0.1688755420497';
    P_START_TIME   timestamp        := TO_TIMESTAMP_TZ('2005-02-01 13:30:00', 'YYYY-MM-DD HH24:MI:SS');
    P_END_TIME   timestamp          := TO_TIMESTAMP_TZ('2005-02-02 17:00:00', 'YYYY-MM-DD HH24:MI:SS');
    P_VERSION_DATE      date            := null;
    P_TIME_ZONE         varchar2(8)     := 'UTC';
    P_MAX_VERSION       varchar2(1)     := 'T';
    P_OFFICE_ID         varchar2(10)    := 'SPK';
begin

    CWMS_20.CWMS_TEXT.DELETE_TS_STD_TEXT(
                P_TSID => P_TSID,
        P_STD_TEXT_ID_MASK => '*',
        P_START_TIME => P_START_TIME,
        P_END_TIME => P_END_TIME,
        P_VERSION_DATE => P_VERSION_DATE,
        P_TIME_ZONE => P_TIME_ZONE,
        P_MAX_VERSION => P_MAX_VERSION,
        P_MIN_ATTRIBUTE => null,
        P_MAX_ATTRIBUTE => null,
        P_OFFICE_ID => P_OFFICE_ID
);

end;