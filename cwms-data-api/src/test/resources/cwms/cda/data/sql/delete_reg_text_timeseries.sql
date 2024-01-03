declare
    P_TSID         varchar2(256) := 'First519402.Flow.Inst.1Hour.0.1688755420497';
    P_VERSION_DATE date           := null;
    P_START_TIME   timestamp        := TO_TIMESTAMP_TZ('2005-01-01 0:30:00', 'YYYY-MM-DD HH24:MI:SS');
    P_END_TIME   timestamp          := TO_TIMESTAMP_TZ('2005-01-02 23:00:00', 'YYYY-MM-DD HH24:MI:SS');
    P_TIME_ZONE    varchar2(8) := 'UTC';
    P_OFFICE_ID    varchar2(10) := 'SPK';
begin
        CWMS_20.CWMS_TEXT.DELETE_TS_TEXT(
            P_TSID => P_TSID,
            P_TEXT_MASK => '*',
            P_START_TIME => P_START_TIME,
            P_END_TIME => P_END_TIME,
            P_VERSION_DATE => P_VERSION_DATE,
            P_TIME_ZONE => P_TIME_ZONE,
            P_MAX_VERSION => 'T',
            P_MIN_ATTRIBUTE => null,
            P_MAX_ATTRIBUTE => null,
            P_OFFICE_ID => P_OFFICE_ID
    );


end;