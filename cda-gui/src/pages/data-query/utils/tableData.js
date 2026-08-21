import dayjs from "dayjs";

export function buildSeriesLookup(rawSeries = []) {
  return new Map(
    rawSeries.map((series) => [
      series.name,
      {
        ...series,
        valuesByDate: new Map((series.values || []).map((value) => [value[0], value])),
      },
    ]),
  );
}

export function buildTableIndex({
  rawSeries = [],
  sortAscending = true,
  timeseriesParams = [],
}) {
  const seriesLookup = buildSeriesLookup(rawSeries);
  const visibleTsids = timeseriesParams.map((param) => param.tsid);
  const dateSet = new Set();

  visibleTsids.forEach((tsid) => {
    const series = seriesLookup.get(tsid);
    series?.values?.forEach((value) => dateSet.add(value[0]));
  });

  return {
    dates: [...dateSet].sort((a, b) => (sortAscending ? a - b : b - a)),
    seriesLookup,
    visibleTsids,
  };
}

export function buildTableRowValues({
  date,
  missingString = "",
  precisionByTsid = new Map(),
  seriesLookup,
  visibleTsids = [],
}) {
  return visibleTsids.map((tsid) => {
    const rawValue = seriesLookup.get(tsid)?.valuesByDate.get(date)?.[1];
    const precision = precisionByTsid.get(tsid) ?? 2;

    if (rawValue === null || rawValue === undefined) return missingString;
    const numericValue = Number(rawValue);
    return Number.isFinite(numericValue)
      ? numericValue.toFixed(precision)
      : String(rawValue);
  });
}

export function buildTableRows({
  dateFormat = "YYYY-MM-DD HH:mm:ss",
  missingString = "",
  rawSeries = [],
  sortAscending = true,
  timeseriesParams = [],
}) {
  const { dates, seriesLookup, visibleTsids } = buildTableIndex({
    rawSeries,
    sortAscending,
    timeseriesParams,
  });
  const precisionByTsid = new Map(
    timeseriesParams.map((param) => [param.tsid, param.rounding ?? 2]),
  );

  return dates.map((date) => ({
    date,
    formattedDate: dayjs(date).format(dateFormat),
    values: buildTableRowValues({
      date,
      missingString,
      precisionByTsid,
      seriesLookup,
      visibleTsids,
    }),
  }));
}

function escapeCsvCell(value) {
  const text = value === null || value === undefined ? "" : String(value);
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

export function buildCsvContent({ rows, timeseriesParams }) {
  const header = ["Date", ...timeseriesParams.map((param) => param.header)];
  return [header, ...rows.map((row) => [row.formattedDate, ...row.values])]
    .map((row) => row.map(escapeCsvCell).join(","))
    .join("\n");
}

export function downloadBlob({ content, fileName, type }) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");

  link.href = url;
  link.download = fileName;
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}
