package cwms.cda.data.dto.binarytimeseries;

import static cwms.cda.data.dao.JsonRatingUtilsTest.readFully;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import cwms.cda.api.enums.VersionType;
import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;

class BinaryTimeSeriesTest {

    @Test
    void testSerialize() throws IOException {
        BinaryTimeSeries.Builder builder = new BinaryTimeSeries.Builder();
        Collection<BinaryTimeSeriesRow> rows = new ArrayList<>();
        BinaryTimeSeriesRow.Builder rowBuilder = new BinaryTimeSeriesRow.Builder();
        long date = 1209333333333L;
        long attr = 1;
        byte[] bytes = "the_byles".getBytes();
        rowBuilder
                .withDataEntryDate(new Date(1209222222222L).toInstant())
                .withMediaType("mediaType")
                .withFilename("file.bin")
                .withBinaryValue(bytes)
                .withDestFlag(0)
                .withValueUrl("http://somehost:80/path?thequery")
        ;

        rows.add(rowBuilder.withDateTime(new Date(date++).toInstant()).build());
        rows.add(rowBuilder.withDateTime(new Date(date++).toInstant()).build());
        rows.add(rowBuilder.withDateTime(new Date(date++).toInstant()).build());

        BinaryTimeSeries bts = builder
                .withOfficeId("SPK")
                .withName("TsBinTestLoc.Flow.Inst.1Hour.0.raw")
                .withIntervalOffset(0L)
                .withTimeZone("PST")
                .withDateVersionType(VersionType.UNVERSIONED)
                .withBinaryValues(rows)
                .build();

        assertNotNull(bts);
        assertEquals(3, bts.getBinaryValues().size());

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        String json = objectMapper.writeValueAsString(bts);
        assertNotNull(json);
    }


    @Test
    void testDeserialize() throws IOException {
        InputStream stream = getClass().getResourceAsStream("/cwms/cda/data/dto/binarytimeseries/binarytimeseries.json");
        assertNotNull(stream);
        String json = readFully(stream);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        BinaryTimeSeries bts = objectMapper.readValue(json, BinaryTimeSeries.class);
        assertNotNull(bts);
        assertEquals(VersionType.UNVERSIONED, bts.getDateVersionType());

        Collection<BinaryTimeSeriesRow> rows = bts.getBinaryValues();
        assertNotNull(rows);
        assertEquals(3, rows.size());

        List<BinaryTimeSeriesRow> rowList = new ArrayList<>(rows);

        BinaryTimeSeriesRow first = rowList.get(0);
        assertNotNull(first);

//        ZonedDateTime start = ZonedDateTime.parse("2008-05-01T15:00:00-00:00[UTC]");

        assertEquals(1209679200000L, first.getDateTime().toEpochMilli() );

        assertEquals("mediaType", first.getMediaType());
        assertEquals("file.bin", first.getFilename());
        assertEquals("binaryData", new String(first.getBinaryValue()));
        assertEquals(2, first.getDestFlag());

        BinaryTimeSeriesRow third = rowList.get(2);
        assertNotNull(third);
        assertEquals(0, third.getDestFlag());

    }


    @Test
    void testDeserialize2() throws IOException {
        InputStream stream = getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_create.json");
        assertNotNull(stream);
        String json = readFully(stream);

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        BinaryTimeSeries bts = objectMapper.readValue(json, BinaryTimeSeries.class);
        assertNotNull(bts);

        Collection<BinaryTimeSeriesRow> rows = bts.getBinaryValues();
        assertNotNull(rows);
        assertEquals(3, rows.size());
        BinaryTimeSeriesRow first = rows.iterator().next();
        assertNotNull(first);

        assertEquals(1209654000000L, first.getDateTime().toEpochMilli() );


    }

    @Test
    void testRoundtrip() throws JsonProcessingException {
        BinaryTimeSeries.Builder builder = new BinaryTimeSeries.Builder();
        Collection<BinaryTimeSeriesRow> rows = new ArrayList<>();
        BinaryTimeSeriesRow.Builder rowBuilder = new BinaryTimeSeriesRow.Builder();
        long date = 1209333333333L;

        rowBuilder
                .withDataEntryDate(new Date(1209222222222L).toInstant())
                .withMediaType("mediaType")
                .withFilename("file.bin")
                .withBinaryValue("binaryData".getBytes())
                .withDestFlag(2)
        ;

        rows.add(rowBuilder.withDateTime(new Date(date++).toInstant()).build());
        rows.add(rowBuilder.withDateTime(new Date(date++).toInstant()).build());
        rows.add(rowBuilder.withDateTime(new Date(date++).toInstant()).build());

        BinaryTimeSeries bts = builder
                .withOfficeId("SPK")
                .withName("TsBinTestLoc.Flow.Inst.1Hour.0.raw")
                .withIntervalOffset(0L)
                .withTimeZone("PST")
                .withBinaryValues(rows)
                .build();

        assertNotNull(bts);
        assertEquals(3, bts.getBinaryValues().size());

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String json = objectMapper.writeValueAsString(bts);

        assertNotNull(json);

        BinaryTimeSeries bts2 = objectMapper.readValue(json, BinaryTimeSeries.class);
        assertNotNull(bts2);
        assertEquals(bts.getOfficeId(), bts2.getOfficeId());
        assertEquals(bts.getName(), bts2.getName());
        assertEquals(bts.getIntervalOffset(), bts2.getIntervalOffset());
        assertEquals(bts.getTimeZone(), bts2.getTimeZone());
        Collection<BinaryTimeSeriesRow> binaryValues = bts.getBinaryValues();
        Collection<BinaryTimeSeriesRow> binaryValues2 = bts2.getBinaryValues();
        assertNotNull(binaryValues);
        assertNotNull(binaryValues2);
        assertEquals(binaryValues.size(), binaryValues2.size());

        for (int i = 0; i < binaryValues.size(); i++) {
            BinaryTimeSeriesRow row = (BinaryTimeSeriesRow) binaryValues.toArray()[i];
            BinaryTimeSeriesRow row2 = (BinaryTimeSeriesRow) binaryValues2.toArray()[i];
            assertEquals(row.getDateTime(), row2.getDateTime());
            assertEquals(row.getDataEntryDate(), row2.getDataEntryDate());
            assertEquals(row.getMediaType(), row2.getMediaType());
            assertEquals(row.getFilename(), row2.getFilename());
            assertEquals(new String(row.getBinaryValue()), new String(row2.getBinaryValue()));
            assertEquals(row.getDestFlag(), row2.getDestFlag());
            assertEquals(row.getValueUrl(), row2.getValueUrl());
        }

    }
}
