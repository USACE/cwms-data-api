package cwms.cda.data.dto.timeseriestext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.junit.jupiter.api.Test;

class StandardTextTimeSeriesRowTest {


    @Test
    void testSerialize() throws JsonProcessingException, ParseException {

        StandardTextTimeSeriesRow row = buildStdRow();
        assertNotNull(row);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(row);
        assertNotNull(json);
        System.out.println(json);


        assertTrue(json.contains("ESTIMATED"));
        assertTrue(json.contains("420"));

    }

    public static StandardTextTimeSeriesRow buildStdRow() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Parse the date string and create a Date object

        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();
        builder.withDateTime(df.parse("2023-01-03 12:05:00"));
        builder.withVersionDate(df.parse("2023-05-02 12:05:00"));
        builder.withDataEntryDate(df.parse("2023-05-02 12:05:00"));
        builder.withAttribute(420L);
        builder.withOfficeId("CWMS");
        builder.withStandardTextId("E");
        builder.withTextValue("ESTIMATED");

        StandardTextTimeSeriesRow row = builder.build();
        return row;
    }

    @Test
    void testRoundTrip() throws JsonProcessingException, ParseException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();
        builder.withDateTime(df.parse("2023-01-01 12:05:00"));
        builder.withVersionDate(df.parse("2023-02-02 12:05:00"));
        builder.withDataEntryDate(df.parse("2023-03-03 12:05:00"));
        builder.withAttribute(420L);
        builder.withOfficeId("CWMS");
        builder.withStandardTextId("E");
        builder.withTextValue("ESTIMATED");

        StandardTextTimeSeriesRow row = builder.build();
        assertNotNull(row);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(row);
        assertNotNull(json);
        System.out.println(json);

        StandardTextTimeSeriesRow row2 = objectMapper.readValue(json, StandardTextTimeSeriesRow.class);
        assertNotNull(row2);

        assertEquals(row.getAttribute(), row2.getAttribute());
        assertEquals(row.getDateTime(), row2.getDateTime());
        assertEquals(row.getDataEntryDate(), row2.getDataEntryDate());
        assertEquals(row.getVersionDate(), row2.getVersionDate());
        assertEquals(row.getStandardTextId(), row2.getStandardTextId());
        assertEquals(row.getTextValue(), row2.getTextValue());

    }

    @Test
    void testEquals() throws ParseException {

        StandardTextTimeSeriesRow row = buildRow();
        assertNotNull(row);
        StandardTextTimeSeriesRow row2 = buildRow();
        assertNotNull(row2);

        assertNotSame(row, row2);
        assertEquals(row, row2);
        assertEquals(row2, row);

    }

    private static StandardTextTimeSeriesRow buildRow() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();

        builder.withDateTime(df.parse("2023-01-03 12:05:00"));
        builder.withVersionDate(df.parse("2023-02-02 12:05:00"));
        builder.withDataEntryDate(df.parse("2023-03-03 12:05:00"));
        builder.withAttribute(420L);
        builder.withOfficeId("CWMS");
        builder.withStandardTextId("E");
        builder.withTextValue("ESTIMATED");

        StandardTextTimeSeriesRow row = builder.build();
        return row;
    }

}