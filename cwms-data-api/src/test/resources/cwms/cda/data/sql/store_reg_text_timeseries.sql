declare
    P_TSID         varchar2(256) := 'TsTextTestLoc.Flow.Inst.1Hour.0.raw';
    P_TEXT         clob           := 'my awesome text ts';
    P_START_TIME   timestamp        := TO_TIMESTAMP_TZ('2005-01-01 02:30:00', 'YYYY-MM-DD HH24:MI:SS');
    P_END_TIME   timestamp          := TO_TIMESTAMP_TZ('2005-01-01 07:00:00', 'YYYY-MM-DD HH24:MI:SS');
    P_VERSION_DATE date           := null;
    P_TIME_ZONE    varchar2(8) := 'UTC';
    P_ATTRIBUTE    number         := null;
    P_OFFICE_ID    varchar2(10) := 'SPK';
begin
    /**
       * Store nonstandard text to a time series. The text can be:
       * <ul>
       *   <li>associated with a "normal" time series with numeric values and quality codes</li>
       *   <li>associated with a binary time series (base parameter = "Binary") that contains images, documents, etc...</li>
       *   <li>the contents of a text time series (base parameter = "Text")</li>
       * </ul>
       * Unlike a "normal" time series, which can have only one value/quality pair at any time/version date combination,
       * binary and text time series can have multiple entries at each time/version date combination.  Entries are retrieved
       * in the order they are stored.
       *
       * @param p_tsid         The time series identifier
       * @param p_text         The text to store.
       * @param p_start_time   The first (or only) time for the text
       * @param p_end_time     The last time for the text. If specified the text is associated with all times from p_start_time to p_end_time (inclusive). Times must already exist for irregular time series.
       * @param p_version_date The version date for the time series.  If not specified or NULL, the minimum or maximum version date (depending on p_max_version) is used.
       * @param p_time_zone    The time zone for p_start_time, p_end_time, and p_version_date. If not specified or NULL, the local time zone of the time series' location is used.
       * @param p_max_version  A flag ('T' or 'F') specifying whether to use the maximum version date if p_version_date is not specified or NULL.
       * @param p_existing     A flag ('T' or 'F') specifying whether to store the text for times that already exist in the specified time series. Used only for regular time series.
       * @param p_non_existing A flag ('T' or 'F') specifying whether to store the text for times that don't already exist in the specified time series. Used only for regular time series.
       * @param p_replace_all  A flag ('T' or 'F') specifying whether to replace any and all existing text with the specified text
       * @param p_attribute    A numeric attribute that can be used for sorting or other purposes
       * @param p_office_id    The office that owns the time series. If not specified or NULL, the session user's default office is used.
       */
    CWMS_20.CWMS_TEXT.STORE_TS_TEXT(
            P_TSID => P_TSID,
            P_TEXT => P_TEXT,
            P_START_TIME => P_START_TIME,
            P_END_TIME => P_END_TIME,
            P_VERSION_DATE => P_VERSION_DATE,
            P_TIME_ZONE => P_TIME_ZONE,
            P_MAX_VERSION => 'T',
            P_EXISTING => 'T',
            P_NON_EXISTING => 'T',
            P_REPLACE_ALL => 'T',
            P_ATTRIBUTE => P_ATTRIBUTE,
            P_OFFICE_ID => P_OFFICE_ID
    );


end;