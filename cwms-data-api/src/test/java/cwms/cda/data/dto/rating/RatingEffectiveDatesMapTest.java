package cwms.cda.data.dto.rating;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

final class RatingEffectiveDatesMapTest {

    @Test
    void testDeserialization() throws Exception {
        InputStream resource =
                this.getClass().getResourceAsStream("/cwms/cda/data/dto/rating/rating_effective_dates.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        ContentType contentType = new ContentType(Formats.JSON);
        RatingEffectiveDatesMap deserialized = Formats.parseContent(contentType, json, RatingEffectiveDatesMap.class);
        Map<String, List<RatingSpecEffectiveDates>> expectedMap = new HashMap<>();
        expectedMap.put("SPK", Arrays.asList(
                new RatingSpecEffectiveDates.Builder()
                        .withRatingSpecId("Abc.Stage;Flow.V1.V1")
                        .withEffectiveDates(new TreeSet<>(Arrays.asList(
                                Instant.parse("2020-03-01T00:00:00Z"),
                                Instant.parse("2021-03-01T00:00:00Z"))))
                        .build(),
                new RatingSpecEffectiveDates.Builder()
                        .withRatingSpecId("Abc2.Stage;Flow.V1.V1")
                        .withEffectiveDates(new TreeSet<>(Arrays.asList(
                                Instant.parse("2020-03-02T00:00:00Z"),
                                Instant.parse("2021-03-02T00:00:00Z"))))
                        .build()
        ));
        expectedMap.put("SWT", Collections.singletonList(
                new RatingSpecEffectiveDates.Builder()
                        .withRatingSpecId("Xyz.Stage2;Flow.V2.V1")
                        .withEffectiveDates(new TreeSet<>(Collections.singletonList(
                                Instant.parse("2022-01-01T00:00:00Z"))))
                        .build()
        ));
        RatingEffectiveDatesMap expected = new RatingEffectiveDatesMap.Builder()
                .withOfficeToSpecDatesMap(expectedMap)
                .build();
        DTOMatch.assertMatch(expected, deserialized);
    }

    @Test
    void testSerializationRoundTrip() {
        Map<String, List<RatingSpecEffectiveDates>> expectedMap = new HashMap<>();
        expectedMap.put("SPK", Arrays.asList(
                new RatingSpecEffectiveDates.Builder()
                        .withRatingSpecId("Abc.Stage;Flow.V1.V1")
                        .withEffectiveDates(new TreeSet<>(Arrays.asList(
                                Instant.parse("2020-03-01T00:00:00Z"),
                                Instant.parse("2021-03-01T00:00:00Z"))))
                        .build(),
                new RatingSpecEffectiveDates.Builder()
                        .withRatingSpecId("Abc2.Stage;Flow.V1.V1")
                        .withEffectiveDates(new TreeSet<>(Arrays.asList(
                                Instant.parse("2020-03-02T00:00:00Z"),
                                Instant.parse("2021-03-02T00:00:00Z"))))
                        .build()
        ));
        expectedMap.put("SWT", Collections.singletonList(
                new RatingSpecEffectiveDates.Builder()
                        .withRatingSpecId("Xyz.Stage2;Flow.V2.V1")
                        .withEffectiveDates(new TreeSet<>(Collections.singletonList(
                                Instant.parse("2022-01-01T00:00:00Z"))))
                        .build()
        ));
        RatingEffectiveDatesMap dto = new RatingEffectiveDatesMap.Builder()
                .withOfficeToSpecDatesMap(expectedMap)
                .build();
        ContentType contentType = new ContentType(Formats.JSON);
        String serialized = Formats.format(contentType, dto);

        RatingEffectiveDatesMap deserialized = Formats.parseContent(contentType, serialized, RatingEffectiveDatesMap.class);
        DTOMatch.assertMatch(dto, deserialized);
    }
}
