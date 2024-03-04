package cwms.cda.data.dto.texttimeseries;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class TextTimeSeriesTest {



    @Test
    void testCanSetTimezone() throws JsonProcessingException {

        TextTimeSeries textTimeSeries = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withName("First519402.Flow.Inst.1Hour.0.1688755420497")
                .withTimeZone("America/New_York")
                .build();

        assertEquals("America/New_York", textTimeSeries.getTimeZone());

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(textTimeSeries);
        assertNotNull(json);

        assertTrue(json.contains("America/New_York"));
    }

    @Test
    void testCanSetIntOffset() throws JsonProcessingException {

        TextTimeSeries textTimeSeries = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withName("First519402.Flow.Inst.1Hour.0.1688755420497")
                .withIntervalOffset(0L)
                .build();

        assertEquals(0L, textTimeSeries.getIntervalOffset());

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(textTimeSeries);
        assertNotNull(json);

        assertTrue(json.contains("\"interval-offset\":0"));

    }

    @Test
    void testCanHoldRegRows() throws JsonProcessingException, ParseException {

        RegularTextTimeSeriesRow row = RegularTextTimeSeriesRowTest.buildRow();
        assertNotNull(row);

        TextTimeSeries textTimeSeries = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withName("First519402.Flow.Inst.1Hour.0.1688755420497")
                .withRow(row).build();

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(textTimeSeries);
        assertNotNull(json);

        assertFalse(json.contains("ESTIMATED"));
        assertFalse(json.contains("CWMS"));
        assertTrue(json.contains("420"));

    }



    @Test
    void testRegDeserialize() throws IOException {
        InputStream stream = DataApiTestIT.class.getClassLoader().getResourceAsStream(
                "cwms/cda/data/dto/text_time_series_reg.json");
        assertNotNull(stream);
        String input = IOUtils.toString(stream, StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        TextTimeSeries tts = om.readValue(input, TextTimeSeries.class);

        assertNotNull(tts);
        assertEquals("SPK", tts.getOfficeId());
        assertNotNull(tts.getRegularTextValues());
        assertFalse(tts.getRegularTextValues().isEmpty());
        RegularTextTimeSeriesRow firstRow = tts.getRegularTextValues().iterator().next();
        assertNotNull(firstRow);
        assertEquals("my awesome text ts", firstRow.getTextValue());

    }





}