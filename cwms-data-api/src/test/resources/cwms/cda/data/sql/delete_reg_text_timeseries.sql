declare
    P_TSID         varchar2(256) := 'First519402.Flow.Inst.1Hour.0.1688755420497';
    P_VERSION_DATE date           := null;
    P_TIME_ZONE    varchar2(8) := 'UTC';
    P_OFFICE_ID    varchar2(10) := 'SPK';
begin
        CWMS_20.CWMS_TEXT.DELETE_TS_TEXT(
            P_TSID => P_TSID,
            P_TEXT_MASK => '*',
            P_START_TIME =>TO_DATE('2005-01-01', 'YYYY-MM-DD'),
            P_END_TIME => TO_DATE('2005-01-02', 'YYYY-MM-DD'),
            P_VERSION_DATE => P_VERSION_DATE,
            P_TIME_ZONE => P_TIME_ZONE,
            P_MAX_VERSION => 'T',
            P_MIN_ATTRIBUTE => null,
            P_MAX_ATTRIBUTE => null,
            P_OFFICE_ID => P_OFFICE_ID
    );


end;