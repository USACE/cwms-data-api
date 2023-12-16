declare
    P_TSID              varchar2(256)   := 'First519402.Flow.Inst.1Hour.0.1688755420497';
    P_START_TIME   timestamp        := TO_TIMESTAMP_TZ('2005-02-01 13:30:00', 'YYYY-MM-DD HH24:MI:SS');
    P_END_TIME   timestamp          := TO_TIMESTAMP_TZ('2005-02-02 17:00:00', 'YYYY-MM-DD HH24:MI:SS');
    P_VERSION_DATE      date            := null;
    P_TIME_ZONE         varchar2(8)     := 'UTC';
    P_MAX_VERSION       varchar2(1)     := 'T';
    P_EXISTING          varchar2(1)     := 'T';
    P_NON_EXISTING      varchar2(1)     := 'F';
    P_REPLACE_ALL       varchar2(1)     := 'F';
    P_ATTRIBUTE         number          := null;
    P_OFFICE_ID         varchar2(10)    := 'SPK';
begin

    CWMS_20.CWMS_TEXT.STORE_TS_STD_TEXT(
            P_TSID => P_TSID,
            P_STD_TEXT_ID => 'E',
            P_START_TIME => P_START_TIME,
            P_END_TIME => P_END_TIME,
            P_VERSION_DATE => P_VERSION_DATE,
            P_TIME_ZONE => P_TIME_ZONE,
            P_MAX_VERSION => P_MAX_VERSION,
            P_EXISTING => P_EXISTING,
            P_NON_EXISTING => P_NON_EXISTING,
            P_REPLACE_ALL => P_REPLACE_ALL,
            P_ATTRIBUTE => P_ATTRIBUTE,
            P_OFFICE_ID => P_OFFICE_ID
    );

end;