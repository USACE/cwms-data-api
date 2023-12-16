package cwms.cda.data.dto.timeSeriesText;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import cwms.cda.formatters.json.JsonV2;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.junit.jupiter.api.Test;

class StandardTextTimeSeriesRowTest {


    @Test
    void testSerialize() throws JsonProcessingException, ParseException {

        StandardTextTimeSeriesRow row = buildStdRow();
        assertNotNull(row);

        String json = JsonV2.buildObjectMapper().writeValueAsString(row);
        assertNotNull(json);
        //System.out.println(json);


        assertTrue(json.contains("ESTIMATED"));
        assertTrue(json.contains("420"));

    }

    public static StandardTextTimeSeriesRow buildStdRow() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Parse the date string and create a Date object
        Date specificDate = df.parse("2023-01-01 12:00:00");

        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();
        builder.withDateTime(df.parse("2023-01-03 12:05:00"));
        builder.withVersionDate(df.parse("2023-05-02 12:05:00"));
        builder.withDataEntryDate(df.parse("2023-05-02 12:05:00"));
        builder.withAttribute(420L);
        StandardTextId stdTxtId = new StandardTextId.Builder().withId("E").withOfficeId("CWMS").build();
        builder.withStandardTextId(stdTxtId);
        StandardTextValue stdTxtVal = new StandardTextValue.Builder()
                .withId(stdTxtId)
                .withStandardText("ESTIMATED").build();
        builder.withStandardTextValue(stdTxtVal);

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
        StandardTextId stdTxtId = new StandardTextId.Builder().withId("E").withOfficeId("CWMS").build();
        builder.withStandardTextId(stdTxtId);
        StandardTextValue stdTxtVal = new StandardTextValue.Builder()
                .withId(stdTxtId)
                .withStandardText("ESTIMATED").build();
        builder.withStandardTextValue(stdTxtVal);

        StandardTextTimeSeriesRow row = builder.build();
        assertNotNull(row);

        String json = JsonV2.buildObjectMapper().writeValueAsString(row);
        assertNotNull(json);
        System.out.println(json);

        StandardTextTimeSeriesRow row2 = JsonV2.buildObjectMapper().readValue(json, StandardTextTimeSeriesRow.class);
        assertNotNull(row2);

        assertEquals(row.getAttribute(), row2.getAttribute());
        assertEquals(row.getDateTime(), row2.getDateTime());
        assertEquals(row.getDataEntryDate(), row2.getDataEntryDate());
        assertEquals(row.getVersionDate(), row2.getVersionDate());
        assertEquals(row.getStandardTextId(), row2.getStandardTextId());
        assertEquals(row.getStandardTextValue(), row2.getStandardTextValue());


    }

    @Test
    void testEquals() throws ParseException {

        StandardTextTimeSeriesRow row = buildRow();
        assertNotNull(row);
        StandardTextTimeSeriesRow row2 = buildRow();
        assertNotNull(row2);

        assertFalse(row == row2);
        assertTrue(row.equals(row2));
        assertTrue(row2.equals(row));

    }

    private static StandardTextTimeSeriesRow buildRow() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StandardTextTimeSeriesRow.Builder builder = new StandardTextTimeSeriesRow.Builder();

            builder.withDateTime(df.parse("2023-01-03 12:05:00"));
            builder.withVersionDate(df.parse("2023-02-02 12:05:00"));
            builder.withDataEntryDate(df.parse("2023-03-03 12:05:00"));
            builder.withAttribute(420L);
            StandardTextId stdTxtId = new StandardTextId.Builder().withId("E").withOfficeId("CWMS").build();
            builder.withStandardTextId(stdTxtId);
            StandardTextValue stdTxtVal = new StandardTextValue.Builder()
                    .withId(stdTxtId)
                    .withStandardText("ESTIMATED").build();
            builder.withStandardTextValue(stdTxtVal);

        StandardTextTimeSeriesRow row = builder.build();
        return row;
    }

}