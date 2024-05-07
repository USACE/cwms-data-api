package cwms.cda.data.dto;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.json.JsonV1;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class TsvDquTest {

    public static final String OFFICE = "SPK";
    public static final String CWMS_TS_ID = "Black Butte.Stor-Top Con.Inst.~1Day.0.Calc-val";
    public static final String UNIT = "ac-ft";

    @Test
    void testGetters() {
        Date dateTime = new Date(1712559600000L);
        Date startDate = new Date(1704067200000L);
        Date endDate = new Date(1735689600000L);
        Date versionDate = new Date(-27079747200000L);
        TsvDqu tsvDqu = new TsvDqu.Builder()
                .withOfficeId(OFFICE)
                .withCwmsTsId(CWMS_TS_ID)
                .withUnitId(UNIT)
                .withDateTime(dateTime)
                .withVersionDate(versionDate)
                .withDataEntryDate(null)
                .withValue(0.0)
                .withQualityCode(0L)
                .withStartDate(startDate)
                .withEndDate(endDate)
                .build();

        assertEquals(OFFICE, tsvDqu.getOfficeId());
        assertEquals(CWMS_TS_ID, tsvDqu.getCwmsTsId());
        assertEquals(UNIT, tsvDqu.getUnitId());
        assertEquals(dateTime, tsvDqu.getDateTime());
        assertEquals(versionDate, tsvDqu.getVersionDate());
        assertNull(tsvDqu.getDataEntryDate());
        assertEquals(0.0, tsvDqu.getValue());
        assertEquals(0L, tsvDqu.getQualityCode());
        assertEquals(startDate, tsvDqu.getStartDate());
        assertEquals(endDate, tsvDqu.getEndDate());

    }

    @Test
    void testJsonRoundtrip() throws JsonProcessingException {
        Date dateTime = new Date(1712559600000L);
        Date startDate = new Date(1704067200000L);
        Date endDate = new Date(1735689600000L);
        Date versionDate = new Date(-27079747200000L);
        TsvDqu tsvDqu = new TsvDqu.Builder()
                .withOfficeId(OFFICE)
                .withCwmsTsId(CWMS_TS_ID)
                .withUnitId(UNIT)
                .withDateTime(dateTime)
                .withVersionDate(versionDate)
                .withDataEntryDate(null)
                .withValue(0.0)
                .withQualityCode(0L)
                .withStartDate(startDate)
                .withEndDate(endDate)
                .build();

        ObjectMapper om = JsonV1.buildObjectMapper();
        String serializedTsvDqu = om.writeValueAsString(tsvDqu);
        assertNotNull(serializedTsvDqu);

        TsvDqu tsvDqu2 = om.readValue(serializedTsvDqu, TsvDqu.class);

        assertEquals(tsvDqu.getOfficeId(), tsvDqu2.getOfficeId());
        assertEquals(tsvDqu.getCwmsTsId(), tsvDqu2.getCwmsTsId());
        assertEquals(tsvDqu.getUnitId(), tsvDqu2.getUnitId());
        assertEquals(tsvDqu.getDateTime(), tsvDqu2.getDateTime());
        assertEquals(tsvDqu.getVersionDate(), tsvDqu2.getVersionDate());
        assertEquals(tsvDqu.getDataEntryDate(), tsvDqu2.getDataEntryDate());
        assertEquals(tsvDqu.getValue(), tsvDqu2.getValue());
        assertEquals(tsvDqu.getQualityCode(), tsvDqu2.getQualityCode());
        assertEquals(tsvDqu.getStartDate(), tsvDqu2.getStartDate());
        assertEquals(tsvDqu.getEndDate(), tsvDqu2.getEndDate());

    }

    @Test
    void testDeserialize() throws IOException {

        InputStream stream = DataApiTestIT.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/tsvdqu.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);
        ObjectMapper om = JsonV1.buildObjectMapper();
        TsvDqu tsvDqu2 = om.readValue(input, TsvDqu.class);
        assertNotNull(tsvDqu2);
        assertEquals(OFFICE, tsvDqu2.getOfficeId());
        assertEquals(CWMS_TS_ID, tsvDqu2.getCwmsTsId());
        assertEquals(UNIT, tsvDqu2.getUnitId());
        assertEquals(1712559600000L, tsvDqu2.getDateTime().getTime());
        assertEquals(-27079747200000L, tsvDqu2.getVersionDate().getTime());
        assertEquals(0.0, tsvDqu2.getValue());
        assertEquals(0L, tsvDqu2.getQualityCode());
        assertEquals(1704067200000L, tsvDqu2.getStartDate().getTime());
        assertEquals(1735689600000L, tsvDqu2.getEndDate().getTime());
    }


}