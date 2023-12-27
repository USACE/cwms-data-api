package cwms.cda.data.dto.timeseriestext;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import java.text.ParseException;
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
        System.out.println(json);

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
        System.out.println(json);

        assertFalse(json.contains("ESTIMATED"));
        assertFalse(json.contains("CWMS"));
        assertTrue(json.contains("420"));

    }


}