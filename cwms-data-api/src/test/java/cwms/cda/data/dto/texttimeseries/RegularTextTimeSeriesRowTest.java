package cwms.cda.data.dto.texttimeseries;

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

class RegularTextTimeSeriesRowTest {


    @Test
    void testSerialize() throws JsonProcessingException, ParseException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Parse the date string and create a Date object
//        Date specificDate = df.parse("2023-01-01 12:00:00");

        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder();
        builder.withDateTime(df.parse("2023-01-03 12:05:00").toInstant());
        builder.withDataEntryDate(df.parse("2023-05-02 12:05:00").toInstant());
        builder.withMediaType("text/plain");
        builder.withQualityCode(1L);
        builder.withDestFlag(2);
        builder.withFilename("myFile.txt");

        builder.withTextValue("stdText");

        RegularTextTimeSeriesRow row = builder.build();
        assertNotNull(row);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(row);
        assertNotNull(json);

        assertTrue(json.contains("stdText"));
    }

    @Test
    void testRoundTrip() throws JsonProcessingException, ParseException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder();
        builder.withDateTime(df.parse("2023-01-01 12:05:00").toInstant());

        builder.withDataEntryDate(df.parse("2023-03-03 12:05:00").toInstant());
        builder.withTextValue("my awesome text ts");
        builder.withMediaType("text/plain");
        builder.withQualityCode(420L);
        builder.withDestFlag(2);
        builder.withFilename("myFile.txt");

        RegularTextTimeSeriesRow row = builder.build();
        assertNotNull(row);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(row);
        assertNotNull(json);

        RegularTextTimeSeriesRow row2 = objectMapper.readValue(json, RegularTextTimeSeriesRow.class);
        assertNotNull(row2);


        assertEquals(row.getDateTime(), row2.getDateTime());
        assertEquals(row.getDataEntryDate(), row2.getDataEntryDate());
        assertEquals(row.getTextValue(), row2.getTextValue());
        assertEquals(row.getMediaType(), row2.getMediaType());
        assertEquals(row.getQualityCode(), row2.getQualityCode());
        assertEquals(row.getDestFlag(), row2.getDestFlag());
        assertEquals(row.getFilename(), row2.getFilename());
        assertEquals(row.getValueUrl(), row2.getValueUrl());


    }

    @Test
    void testEquals() throws ParseException {

        RegularTextTimeSeriesRow row = buildRow();
        assertNotNull(row);
        RegularTextTimeSeriesRow row2 = buildRow();
        assertNotNull(row2);

        assertNotSame(row, row2);
        assertEquals(row, row2);
        assertEquals(row2, row);

    }

    static RegularTextTimeSeriesRow buildRow() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        RegularTextTimeSeriesRow.Builder builder = new RegularTextTimeSeriesRow.Builder();
        builder.withDateTime(df.parse("2023-01-03 12:05:00").toInstant());
        builder.withDataEntryDate(df.parse("2023-03-03 12:05:00").toInstant());
        builder.withTextValue("my awesome text ts");
        builder.withMediaType("text/plain");
        builder.withQualityCode(420L);
        builder.withDestFlag(2);
        builder.withFilename("myFile.txt");
        builder.withValueUrl("http://example.com/cwms-data/timeseries/text/ignored?text-id=someId&office-id=SPK&value=true");

        return builder.build();
    }

}