declare
    timeseries_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(timeseries_exists,-20003);
begin

  cwms_ts.create_ts(?,?, ?);
exception
  when timeseries_exists then null
    ;
end;