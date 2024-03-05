package cwms.cda.data.dto.binarytimeseries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Date;
import org.junit.jupiter.api.Test;

class BinaryTimeSeriesRowTest {

    @Test
    void testCanBuild(){
        BinaryTimeSeriesRow.Builder builder = new BinaryTimeSeriesRow.Builder();
        BinaryTimeSeriesRow row = builder
                .build();
        assertNotNull(row);

    }
    @Test
    void testCreateNulls(){
        BinaryTimeSeriesRow.Builder builder = new BinaryTimeSeriesRow.Builder();
        BinaryTimeSeriesRow row = builder.withDateTime(null)
                .withDataEntryDate(null)
                .withAttribute(null)
                .withMediaType(null)
                .withFileExtension(null)
                .withBinaryValue(null)
                .build();
        assertNotNull(row);

    }

    @Test
    void testCreate(){
        BinaryTimeSeriesRow.Builder builder = new BinaryTimeSeriesRow.Builder();
        BinaryTimeSeriesRow row = builder
                .withDateTime(new Date(3333333333L).toInstant())
                .withDataEntryDate(new Date(2222222222L).toInstant())
                .withAttribute(34L)
                .withMediaType("mediaType")
                .withFileExtension(".bin")
                .withBinaryValue("binaryData".getBytes())
                .build();
        assertNotNull(row);
        assertEquals(3333333333L, Date.from(row.getDateTime()).getTime());
        assertEquals(2222222222L, Date.from(row.getDataEntryDate()).getTime());
        assertEquals(34L, row.getAttribute());
        assertEquals("mediaType", row.getMediaType());
        assertEquals(".bin", row.getFileExtension());
        assertEquals("binaryData", new String(row.getBinaryValue()));
    }


}
