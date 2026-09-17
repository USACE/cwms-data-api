package cwms.cda.api;

import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;

/** Reserve read capacity while potentially large responses are still being written. */
final class TimeSeriesExportPolicy {
    private static final int PAGED_RESPONSE_THRESHOLD = 5000;

    private TimeSeriesExportPolicy() {
    }

    static boolean isExport(ContentType contentType, String cursor, int pageSize) {
        if (Formats.CSV.equals(contentType.getType())
                || !"2".equals(contentType.getParameters().get("version"))) {
            return true;
        }
        if (cursor != null && !cursor.isEmpty()) {
            String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
            if (parts.length > 1) {
                pageSize = Integer.parseInt(parts[parts.length - 1]);
            }
        }
        return pageSize < 0 || pageSize > PAGED_RESPONSE_THRESHOLD;
    }
}
