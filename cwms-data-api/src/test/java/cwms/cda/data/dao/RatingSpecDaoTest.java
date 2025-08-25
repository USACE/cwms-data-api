package cwms.cda.data.dao;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import cwms.cda.data.dto.rating.RatingEffectiveDatesMap;
import cwms.cda.data.dto.rating.RatingSpecEffectiveDates;
import java.time.Instant;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.formatters.json.JsonV2;
import java.sql.SQLException;
import java.util.Collection;
import org.jooq.DSLContext;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class RatingSpecDaoTest {

    public static final String OFFICE_ID = "SWT";

    @Test
    @Disabled
    void testRetrieveRatingSpecs() throws SQLException, JsonProcessingException {
        DSLContext lrl = getDslContext(getConnection(), OFFICE_ID);

        RatingSpecDao dao = new RatingSpecDao(lrl);
        Collection<RatingSpec> ratingSpecs = dao.retrieveRatingSpecs(OFFICE_ID, "^ARTH");
        assertNotNull(ratingSpecs);
        assertFalse(ratingSpecs.isEmpty());

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String body = objectMapper.writeValueAsString(ratingSpecs);
        assertNotNull(body);

    }

    @Test
    void testBuildRatingSpecDatesMap() {
        NavigableMap<String, NavigableMap<String, NavigableSet<Instant>>> officeToSpecDatesMap = new TreeMap<>();
        NavigableMap<String, NavigableSet<Instant>> specDatesMap = new TreeMap<>();
        NavigableSet<Instant> dates1 = new java.util.TreeSet<>();
        dates1.add(Instant.parse("2023-01-01T00:00:00Z"));
        dates1.add(Instant.parse("2023-01-02T00:00:00Z"));
        specDatesMap.put("SPEC1", dates1);
        NavigableSet<Instant> dates2 = new java.util.TreeSet<>();
        dates2.add(Instant.parse("2023-01-03T00:00:00Z"));
        dates2.add(Instant.parse("2023-01-04T00:00:00Z"));
        specDatesMap.put("SPEC2", dates2);
        officeToSpecDatesMap.put("SPK", specDatesMap);
        NavigableMap<String, NavigableSet<Instant>> specDatesMap2 = new TreeMap<>();
        NavigableSet<Instant> dates3 = new java.util.TreeSet<>();
        dates3.add(Instant.parse("2023-01-01T00:00:00Z"));
        dates3.add(Instant.parse("2023-01-02T00:00:00Z"));
        specDatesMap2.put("SPECA", dates3);
        NavigableSet<Instant> dates4 = new java.util.TreeSet<>();
        dates4.add(Instant.parse("2023-01-03T00:00:00Z"));
        dates4.add(Instant.parse("2023-01-04T00:00:00Z"));
        specDatesMap2.put("SPECB", dates4);
        officeToSpecDatesMap.put("SWT", specDatesMap2);
        RatingEffectiveDatesMap result = RatingSpecDao.buildRatingEffectiveDatesMap(officeToSpecDatesMap);
        assertNotNull(result);
        assertFalse(result.getOfficeToSpecDates().isEmpty());
        assertFalse(result.getOfficeToSpecDates().get("SPK").isEmpty());
        assertFalse(result.getOfficeToSpecDates().get("SWT").isEmpty());
        RatingSpecEffectiveDates spec1Dates = result.getOfficeToSpecDates().get("SPK").stream()
                .filter(spec -> spec.getRatingSpecId().equals("SPEC1"))
                .findFirst()
                .orElse(null);
        RatingSpecEffectiveDates spec2Dates = result.getOfficeToSpecDates().get("SPK").stream()
                .filter(spec -> spec.getRatingSpecId().equals("SPEC2"))
                .findFirst()
                .orElse(null);
        RatingSpecEffectiveDates specADates = result.getOfficeToSpecDates().get("SWT").stream()
                .filter(spec -> spec.getRatingSpecId().equals("SPECA"))
                .findFirst()
                .orElse(null);
        RatingSpecEffectiveDates specBDates = result.getOfficeToSpecDates().get("SWT").stream()
                .filter(spec -> spec.getRatingSpecId().equals("SPECB"))
                .findFirst()
                .orElse(null);
        assertNotNull(spec1Dates);
        assertNotNull(spec2Dates);
        assertNotNull(specADates);
        assertNotNull(specBDates);
        assertTrue(spec1Dates.getEffectiveDates().contains(Instant.parse("2023-01-01T00:00:00Z")));
        assertTrue(spec1Dates.getEffectiveDates().contains(Instant.parse("2023-01-02T00:00:00Z")));
        assertTrue(spec2Dates.getEffectiveDates().contains(Instant.parse("2023-01-03T00:00:00Z")));
        assertTrue(spec2Dates.getEffectiveDates().contains(Instant.parse("2023-01-04T00:00:00Z")));
        assertTrue(specADates.getEffectiveDates().contains(Instant.parse("2023-01-01T00:00:00Z")));
        assertTrue(specADates.getEffectiveDates().contains(Instant.parse("2023-01-02T00:00:00Z")));
        assertTrue(specBDates.getEffectiveDates().contains(Instant.parse("2023-01-03T00:00:00Z")));
        assertTrue(specBDates.getEffectiveDates().contains(Instant.parse("2023-01-04T00:00:00Z")));
    }


}