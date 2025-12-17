import dayjs from "dayjs";
import { range } from ".";

export function getYearRange(start_year, end_year, FORMAT = "YYYY") {
  let _start = dayjs().set("year", start_year);
  let _end = dayjs();
  if (end_year) _end.set("year", end_year);

  let _years = [];
  while (_start.unix() <= _end.unix()) {
    _years.push(_start.format(FORMAT));
    _start = _start.set("year", _start.year() + 1);
  }
  return _years;
}

export function getMonthRange({
  selectedProject,
  selectedMonth,
  selectedYear,
  lookback_year = 1994,
}) {
  const current_dt = dayjs();
  if (!selectedMonth) selectedMonth = current_dt.format("MMM");
  if (!selectedYear) selectedYear = current_dt.year();
  let _range;
  if (selectedYear == current_dt.year()) {
    let _max_month = current_dt.month();
    // Return months that have passed
    // Month is zero indexed
    _range = range(0, _max_month + 1);
  } else if (selectedYear == lookback_year) {
    // Return months that remained given a lookback_year
    _range = range(10, 12);
  } else {
    _range = range(0, 12);
  }
  // Return all 12 months otherwise
  return _range.map((m) => {
    return dayjs().set("month", m).format("MMM");
  });
}
