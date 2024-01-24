package cwms.cda.data.dto.texttimeseries;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

class TextTimeSeriesTest {

    @Test
    void testCanHoldStdRows() throws JsonProcessingException, ParseException {

        StandardTextTimeSeriesRow row = StandardTextTimeSeriesRowTest.buildStdRow();
        assertNotNull(row);

        TextTimeSeries textTimeSeries = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                        .withId("First519402.Flow.Inst.1Hour.0.1688755420497")
                                .withStdRow(row).build();


        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(textTimeSeries);
        assertNotNull(json);

        assertTrue(json.contains("ESTIMATED"));
        assertTrue(json.contains("CWMS"));
        assertTrue(json.contains("420"));

    }

    @Test
    void testCanHoldRegRows() throws JsonProcessingException, ParseException {

        RegularTextTimeSeriesRow row = RegularTextTimeSeriesRowTest.buildRow();
        assertNotNull(row);

        TextTimeSeries textTimeSeries = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withId("First519402.Flow.Inst.1Hour.0.1688755420497")
                .withRow(row).build();


        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(textTimeSeries);
        assertNotNull(json);

        assertFalse(json.contains("ESTIMATED"));
        assertFalse(json.contains("CWMS"));
        assertTrue(json.contains("420"));

    }

    @Test
    void testStdDeserialize() throws IOException {

        String input = IOUtils.toString( DataApiTestIT.class.getClassLoader().getResourceAsStream("cwms/cda/data/dto/text_time_series_std.json"), StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        TextTimeSeries tts = om.readValue(input, TextTimeSeries.class);
        assertNotNull(tts);

        assertEquals("SPK",tts.getOfficeId());
        assertNull(tts.getRegularTextValues());
        assertNotNull(tts.getStandardTextValues());
        assertFalse(tts.getStandardTextValues().isEmpty());
        StandardTextTimeSeriesRow firstRow = tts.getStandardTextValues().iterator().next();
        assertNotNull(firstRow);
        assertEquals("A", firstRow.getStandardTextId());

    }

    @Test
    void testRegDeserialize() throws IOException {
        String input = IOUtils.toString( DataApiTestIT.class.getClassLoader().getResourceAsStream("cwms/cda/data/dto/text_time_series_reg.json"), StandardCharsets.UTF_8);

        ObjectMapper om = JsonV2.buildObjectMapper();
        TextTimeSeries tts = om.readValue(input, TextTimeSeries.class);

        assertNotNull(tts);
        assertEquals("SPK",tts.getOfficeId());
        assertNull(tts.getStandardTextValues());
        assertNotNull(tts.getRegularTextValues());
        assertFalse(tts.getRegularTextValues().isEmpty());
        RegularTextTimeSeriesRow firstRow = tts.getRegularTextValues().iterator().next();
        assertNotNull(firstRow);
        assertEquals("my awesome text ts", firstRow.getTextValue());

    }


}