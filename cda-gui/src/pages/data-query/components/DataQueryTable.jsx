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
  const mobileGridTemplateColumns = `12rem repeat(${visibleMobileParams.length}, minmax(8rem, 1fr))`;

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
      <div className="border-b border-slate-200 px-3 py-2 text-sm text-slate-600">
        Showing {rows.length.toLocaleString()} timestamps. Rows are rendered as you
        scroll.
      </div>

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
        <table className="w-full min-w-[720px] border-collapse text-sm md:table">
          <thead className="sticky top-0 z-10 bg-slate-100 text-left">
            <tr>
              <th className="w-48 border-b border-slate-200 px-3 py-2 font-semibold">
                Date & Time (Local)
              </th>
              {timeseriesParams.map((param) => (
                <th
                  key={param.tsid}
                  className="hidden border-b border-slate-200 px-3 py-2 font-semibold md:table-cell"
                >
                  {param.header}
                </th>
              ))}
              {visibleMobileParams.map((param) => (
                <th
                  key={`mobile-${param.tsid}`}
                  className="border-b border-slate-200 px-3 py-2 font-semibold md:hidden"
                >
                  {param.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody
            style={{
              display: "block",
              height: `${totalSize}px`,
              position: "relative",
            }}
          >
            {virtualRows.map((virtualRow) => {
              const row = rows[virtualRow.index];
              return (
                <tr
                  key={row.date}
                  className="absolute left-0 grid w-full grid-cols-[12rem_repeat(2,minmax(8rem,1fr))] border-b border-slate-100 even:bg-slate-50 md:table-row"
                  style={{
                    gridTemplateColumns: mobileGridTemplateColumns,
                    height: `${virtualRow.size}px`,
                    transform: `translateY(${virtualRow.start}px)`,
                  }}
                >
                  <td className="px-3 py-2 font-mono text-xs text-slate-700 md:w-48">
                    {row.formattedDate}
                  </td>
                  {row.values.map((value, index) => (
                    <td
                      key={timeseriesParams[index].tsid}
                      className="hidden px-3 py-2 text-right font-mono text-xs md:table-cell"
                    >
                      {value}
                    </td>
                  ))}
                  {visibleMobileIndexes.map((index) => (
                    <td
                      key={`mobile-${timeseriesParams[index].tsid}`}
                      className="px-3 py-2 text-right font-mono text-xs md:hidden"
                    >
                      {row.values[index]}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
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
