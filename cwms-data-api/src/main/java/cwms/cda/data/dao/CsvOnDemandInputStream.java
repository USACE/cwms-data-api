package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.csv.TimeSeriesCsv;
import cwms.cda.data.dto.csv.TimeSeriesCsvRow;
import cwms.cda.formatters.csv.CsvConfiguration;
import cwms.cda.formatters.csv.CsvV1;
import org.jooq.Cursor;
import org.jooq.Record4;
import org.jooq.exception.DataAccessException;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// InputStream that renders CSV rows on-demand from a jOOQ Cursor
final class CsvOnDemandInputStream extends InputStream {
    private final Cursor<? extends Record4<Timestamp, Double, BigDecimal, Timestamp>> cursor;
    private final Iterator<? extends Record4<Timestamp, Double, BigDecimal, Timestamp>> it;
    private final CsvV1 csv;
    private final String tsIdStr;
    private final String officeId;
    private final String units;
    private final Timestamp versionTs;
    private final int rowsPerBuffer;
    private final CsvConfiguration csvConfiguration;
    private final CsvConfiguration rowConfiguration;

    private byte[] buffer = new byte[0];
    private int bufPos = 0;
    private boolean first = true;
    private boolean closed = false;

    CsvOnDemandInputStream(Cursor<? extends Record4<Timestamp, Double, BigDecimal, Timestamp>> cursor,
                           CsvV1 csv,
                           String tsIdStr,
                           String officeId,
                           String units,
                           Timestamp versionTs,
                           CsvConfiguration csvConfiguration,
                           Integer rowsPerBuffer) {
        this.cursor = cursor;
        this.it = cursor.iterator();
        this.csv = csv;
        this.tsIdStr = tsIdStr;
        this.officeId = officeId;
        this.units = units;
        this.versionTs = versionTs;
        this.csvConfiguration = csvConfiguration;
        this.rowConfiguration = new CsvConfiguration.Builder()
                .from(csvConfiguration)
                .withMetadataIncluded(false)
                .build();

        int rpb = rowsPerBuffer == null ? 1 : rowsPerBuffer;
        this.rowsPerBuffer = rpb > 0 ? rpb : 1;
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int r = read(one, 0, 1);
        return r == -1 ? -1 : (one[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (b == null) {
            throw new NullPointerException("Buffer is null");
        }
        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        int totalCopied = 0;
        while (len > 0) {
            if (bufPos >= buffer.length) {
                if (!fillBuffer()) {
                    break; // EOF
                }
            }

            int toCopy = Math.min(len, buffer.length - bufPos);
            System.arraycopy(buffer, bufPos, b, off, toCopy);
            bufPos += toCopy;
            off += toCopy;
            len -= toCopy;
            totalCopied += toCopy;
        }

        return totalCopied == 0 ? -1 : totalCopied;
    }

    private boolean fillBuffer() {
        if (!it.hasNext()) {
            buffer = new byte[0];
            bufPos = 0;
            return false;
        }

        List<TimeSeriesCsvRow> batch = new ArrayList<>(rowsPerBuffer);
        int produced = 0;

        while (it.hasNext() && produced < rowsPerBuffer) {
            Record4<Timestamp, Double, BigDecimal, Timestamp> r = it.next();

            Timestamp ts = r.value1();
            Double val = r.value2();
            BigDecimal qualityCode = r.value3();
            Timestamp dataEntryDate = r.value4();

            TimeSeriesCsvRow row = new TimeSeriesCsvRow.Builder()
                    .withDateTime(ts == null ? null : ts.toInstant())
                    .withValue(val)
                    .withQualityCode(qualityCode == null ? null : qualityCode.intValue())
                    .withDataEntryDate(dataEntryDate == null ? null : dataEntryDate.toInstant())
                    .withUnits(units)
                    .build();

            batch.add(row);
            produced++;
        }

        if (batch.isEmpty()) {
            buffer = new byte[0];
            bufPos = 0;
            return false;
        }

        TimeSeriesCsv container = new TimeSeriesCsv.Builder()
                .withTimeSeriesId(tsIdStr)
                .withOfficeId(officeId)
                .withVersionDate(versionTs == null ? null : versionTs.toInstant().toString())
                .withRows(batch)
                .build();

        String rendered = first
                ? csv.format(container, csvConfiguration)
                : csv.format(container, rowConfiguration);

        if (first) {
            first = false;
        } else {
            // Remove header from subsequent writes
            int headerEnd = rendered.indexOf('\n');
            // Check for \r\n as well
            if (headerEnd != -1) {
                if (headerEnd > 0 && rendered.charAt(headerEnd - 1) == '\r') {
                    // It was \r\n, skip the \n and start at headerEnd + 1
                }
                rendered = rendered.substring(headerEnd + 1);
            }
        }
        buffer = rendered.getBytes(StandardCharsets.UTF_8);
        bufPos = 0;
        return buffer.length > 0;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                cursor.close();
            } catch (DataAccessException ex) {
                FluentLogger.forEnclosingClass().atWarning().withCause(ex).log("Error closing database cursor");
            }
        }
    }
}
