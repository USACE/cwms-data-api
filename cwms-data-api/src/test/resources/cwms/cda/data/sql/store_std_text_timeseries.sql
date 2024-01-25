begin

  /**
  * Store standard text for all times in a time window to a time series. The text can be:
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
  * @param p_std_text_id  The identifier of the standard text to store.
  * @param p_start_time   The first (or only) time for the text
  * @param p_end_time     The last time for the text. If specified the text is associated with all times from p_start_time to p_end_time (inclusive). Times must already exist for irregular time series.
  * @param p_version_date The version date for the time series.  If not specified or NULL, the minimum or maximum version date (depending on p_max_version) is used.
  * @param p_time_zone    The time zone for p_start_time, p_end_time, and p_version_date. If not specified or NULL, the local time zone of the time series' location is used.
  * @param p_max_version  A flag ('T' or 'F') specifying whether to use the maximum version date if p_version_date is not specifed or NULL.
  * @param p_existing     A flag ('T' or 'F') specifying whether to store the text for times that already exist in the specified time series. Used only for regular time series.
  * @param p_non_existing A flag ('T' or 'F') specifying whether to store the text for times that don't already exist in the specified time series. Used only for regular time series.
  * @param p_replace_all  A flag ('T' or 'F') specifying whether to replace any and all existing standard text with the specified text
  * @param p_attribute    A numeric attribute that can be used for sorting or other purposes
  * @param p_office_id    The office that owns the time series. If not specified or NULL, the session user's default office is used.
  */

    CWMS_20.CWMS_TEXT.STORE_TS_STD_TEXT(
            P_TSID => 'TsTextTestLoc.Flow.Inst.1Hour.0.raw',
            P_STD_TEXT_ID => 'E',
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