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
                .withMediaType(null)
                .withFilename(null)
                .withBinaryValue(null)
                .withValueUrl(null)
                .build();
        assertNotNull(row);

    }

    @Test
    void testCreate()  {
        BinaryTimeSeriesRow.Builder builder = new BinaryTimeSeriesRow.Builder();
        BinaryTimeSeriesRow row = builder
                .withDateTime(new Date(3333333333L).toInstant())
                .withDataEntryDate(new Date(2222222222L).toInstant())
                .withMediaType("mediaType")
                .withFilename("file.bin")
                .withBinaryValue("binaryData".getBytes())
                .withDestFlag(0)
                .withValueUrl("http://somehost:80/path?thequery")
                .build();
        assertNotNull(row);
        assertEquals(3333333333L, Date.from(row.getDateTime()).getTime());
        assertEquals(2222222222L, Date.from(row.getDataEntryDate()).getTime());

        assertEquals("mediaType", row.getMediaType());
        assertEquals("file.bin", row.getFilename());
        assertEquals("binaryData", new String(row.getBinaryValue()));
    }


}
