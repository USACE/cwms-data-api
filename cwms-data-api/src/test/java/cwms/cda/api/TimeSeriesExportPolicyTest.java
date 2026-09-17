package cwms.cda.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.ContentType;
import org.junit.jupiter.api.Test;

class TimeSeriesExportPolicyTest {
    @Test
    void batchesAndLegacyFormatsCannotBypassExportCapacity() {
        assertTrue(TimeSeriesExportPolicy.isExport(new ContentType("text/csv"), "", 500));
        assertTrue(TimeSeriesExportPolicy.isExport(new ContentType("application/json;version=1"), "", 1));
        ContentType json = new ContentType("application/json;version=2");
        assertTrue(TimeSeriesExportPolicy.isExport(json, "", -1));
        assertTrue(TimeSeriesExportPolicy.isExport(json, "", 100000));
        assertFalse(TimeSeriesExportPolicy.isExport(json, "", 5000));
        assertFalse(TimeSeriesExportPolicy.isExport(new ContentType("application/xml;version=2"), "", 0));
    }

    @Test
    void continuationUsesItsEmbeddedPageSize() {
        ContentType json = new ContentType("application/json;version=2");
        String large = CwmsDTOPaginated.encodeCursor("1704067200000", 100000, 1000000);
        String small = CwmsDTOPaginated.encodeCursor("1704067200000", 500, 1000000);
        assertTrue(TimeSeriesExportPolicy.isExport(json, large, 1));
        assertFalse(TimeSeriesExportPolicy.isExport(json, small, 100000));
    }
}
