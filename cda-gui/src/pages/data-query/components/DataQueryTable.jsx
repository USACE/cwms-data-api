import { useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import PropTypes from "prop-types";
import { buildTableRows } from "../utils/tableData";

function getDefaultMobileColumns(timeseriesParams) {
  return timeseriesParams.slice(0, 2).map((param) => param.tsid);
}

function normalizeMobileColumns(columns, timeseriesParams) {
  const validTsids = new Set(timeseriesParams.map((param) => param.tsid));
  const fallback = getDefaultMobileColumns(timeseriesParams);

  return [
    ...new Set([...columns.filter((tsid) => validTsids.has(tsid)), ...fallback]),
  ].slice(0, Math.min(2, timeseriesParams.length));
}

export default function DataQueryTable({
  dateFormat = "YYYY-MM-DD HH:mm:ss",
  missingString = "---",
  rawSeries,
  sortAscending,
  timeseriesParams,
}) {
  const parentRef = useRef(null);
  const [mobileColumns, setMobileColumns] = useState(() =>
    getDefaultMobileColumns(timeseriesParams),
  );

  useEffect(() => {
    setMobileColumns((current) => normalizeMobileColumns(current, timeseriesParams));
  }, [timeseriesParams]);

  const rows = useMemo(
    () =>
      buildTableRows({
        dateFormat,
        missingString,
        rawSeries,
        sortAscending,
        timeseriesParams,
      }),
    [dateFormat, missingString, rawSeries, sortAscending, timeseriesParams],
  );

  const visibleMobileParams = useMemo(
    () =>
      mobileColumns
        .map((tsid) => timeseriesParams.find((param) => param.tsid === tsid))
        .filter(Boolean),
    [mobileColumns, timeseriesParams],
  );

  const visibleMobileIndexes = useMemo(
    () =>
      visibleMobileParams.map((param) =>
        timeseriesParams.findIndex((item) => item.tsid === param.tsid),
      ),
    [timeseriesParams, visibleMobileParams],
  );

  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    estimateSize: () => 36,
    getScrollElement: () => parentRef.current,
    overscan: 12,
  });

  const virtualRows = rowVirtualizer.getVirtualItems();
  const totalSize = rowVirtualizer.getTotalSize();
  const mobileColumnSlots = Array.from({
    length: Math.min(2, timeseriesParams.length),
  });
  const desktopGridStyle = {
    gridTemplateColumns: `12rem repeat(${timeseriesParams.length}, minmax(8rem, 1fr))`,
    minWidth: `${12 + timeseriesParams.length * 8}rem`,
  };
  const mobileGridStyle = {
    gridTemplateColumns: `12rem repeat(${visibleMobileParams.length}, minmax(8rem, 1fr))`,
    minWidth: `${12 + visibleMobileParams.length * 8}rem`,
  };

  if (!timeseriesParams.length) {
    return null;
  }

  if (!rows.length) {
    return (
      <div className="rounded border border-slate-200 bg-white p-4 text-center text-sm text-slate-600">
        No table rows found for the selected time series.
      </div>
    );
  }

  return (
    <div className="rounded border border-slate-200 bg-white">
      <div className="grid gap-3 border-b border-slate-200 p-3 md:hidden">
        {mobileColumnSlots.map((_, slot) => (
          <label key={slot} className="grid gap-1 text-sm text-slate-700">
            <span>Column {slot + 1}</span>
            <select
              className="w-full rounded border border-slate-300 px-2 py-2"
              value={mobileColumns[slot] || ""}
              onChange={(event) => {
                const next = [...mobileColumns];
                next[slot] = event.target.value;
                setMobileColumns(normalizeMobileColumns(next, timeseriesParams));
              }}
            >
              {timeseriesParams.map((param) => (
                <option key={param.tsid} value={param.tsid}>
                  {param.header}
                </option>
              ))}
            </select>
          </label>
        ))}
      </div>

      <div ref={parentRef} className="max-h-[60vh] overflow-auto">
        <div className="hidden text-sm md:block" role="table">
          <div
            className="sticky top-0 z-10 grid bg-slate-100 text-left font-semibold"
            role="row"
            style={desktopGridStyle}
          >
            <div className="border-b border-slate-200 px-3 py-2" role="columnheader">
              Date & Time (Local)
            </div>
            {timeseriesParams.map((param) => (
              <div
                key={param.tsid}
                className="border-b border-slate-200 px-3 py-2"
                role="columnheader"
              >
                {param.header}
              </div>
            ))}
          </div>
          <div
            className="relative"
            role="rowgroup"
            style={{ height: `${totalSize}px`, minWidth: desktopGridStyle.minWidth }}
          >
            {virtualRows.map((virtualRow) => {
              const row = rows[virtualRow.index];
              return (
                <div
                  key={row.date}
                  className="absolute left-0 grid w-full border-b border-slate-100 odd:bg-white even:bg-slate-50"
                  role="row"
                  style={{
                    ...desktopGridStyle,
                    height: `${virtualRow.size}px`,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  <div
                    className="px-3 py-2 font-mono text-xs text-slate-700"
                    role="cell"
                  >
                    {row.formattedDate}
                  </div>
                  {row.values.map((value, index) => (
                    <div
                      key={timeseriesParams[index].tsid}
                      className="px-3 py-2 text-right font-mono text-xs"
                      role="cell"
                    >
                      {value}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        </div>

        <div className="text-sm md:hidden" role="table">
          <div
            className="sticky top-0 z-10 grid bg-slate-100 text-left font-semibold"
            role="row"
            style={mobileGridStyle}
          >
            <div className="border-b border-slate-200 px-3 py-2" role="columnheader">
              Date & Time (Local)
            </div>
            {visibleMobileParams.map((param) => (
              <div
                key={param.tsid}
                className="border-b border-slate-200 px-3 py-2"
                role="columnheader"
              >
                {param.header}
              </div>
            ))}
          </div>
          <div
            className="relative"
            role="rowgroup"
            style={{ height: `${totalSize}px`, minWidth: mobileGridStyle.minWidth }}
          >
            {virtualRows.map((virtualRow) => {
              const row = rows[virtualRow.index];
              return (
                <div
                  key={row.date}
                  className="absolute left-0 grid w-full border-b border-slate-100 odd:bg-white even:bg-slate-50"
                  role="row"
                  style={{
                    ...mobileGridStyle,
                    height: `${virtualRow.size}px`,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  <div
                    className="px-3 py-2 font-mono text-xs text-slate-700"
                    role="cell"
                  >
                    {row.formattedDate}
                  </div>
                  {visibleMobileIndexes.map((index) => (
                    <div
                      key={timeseriesParams[index].tsid}
                      className="px-3 py-2 text-right font-mono text-xs"
                      role="cell"
                    >
                      {row.values[index]}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

DataQueryTable.propTypes = {
  dateFormat: PropTypes.string,
  missingString: PropTypes.string,
  rawSeries: PropTypes.array,
  sortAscending: PropTypes.bool.isRequired,
  timeseriesParams: PropTypes.arrayOf(
    PropTypes.shape({
      header: PropTypes.string.isRequired,
      rounding: PropTypes.number,
      tsid: PropTypes.string.isRequired,
    }),
  ).isRequired,
};
