package cwms.cda.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import cwms.cda.data.dto.TimeSeries;

class DateUtilsTest {


    @ParameterizedTest
    @ArgumentsSource(FullDatesArguments.class)
    void test_iso_dates_from_user( String inputDate, ZoneId tz, ZonedDateTime expected){
        ZonedDateTime result = DateUtils.parseUserDate(inputDate, tz,null); // now not used in this case force npe if
                                                                            // someone accidentally sets that up.
        // isEqual returns true if the instant is the same.  They can have different zones.
        assertTrue(result.isEqual(expected), "Provided date input not correctly matched");
    }

    @ParameterizedTest
    @ArgumentsSource(DateWithoutZoneArguments.class)
    void test_iso_dates_without_zone_from_user( String inputDate, ZoneId tz, ZonedDateTime expected){
        ZonedDateTime result = DateUtils.parseUserDate(inputDate, tz,null);
        // This checks that the two times refer to the same instant on the time-line.
        assertTrue(result.isEqual(expected), "Provided date input not correctly matched");
        // All the args in this test don't have a timezone, so the result should be in the provided fallback zone.
        ZoneId zone = result.getZone();
        assertEquals(zone, tz, "When string does not contain a tz the provided tz should be used");
    }


    @ParameterizedTest
    @ArgumentsSource(PeriodArguments.class)
    void test_iso_period_from_user(ZonedDateTime now, String inputPeriod, ZoneId tz, ZonedDateTime expected){
        ZonedDateTime result = DateUtils.parseUserDate(inputPeriod, tz, now);
        assertTrue(result.isEqual(expected), "User provided input not correctly matched to expected result");
    }

    @ParameterizedTest
    @ArgumentsSource(DurationArguments.class)
    void test_iso_duration_from_user(ZonedDateTime now, String inputPeriod, ZoneId tz, ZonedDateTime expected){
        ZonedDateTime result = DateUtils.parseUserDate(inputPeriod, tz, now);
        assertTrue(result.isEqual(expected), "User provided input not correctly matched to expected result");
    }


    @Test
    void test_time_series_format_and_parse(){
        // In order to test that the format used by TimeSeries can be parsed by DateUtils we will
        // create a date
        // create a formatter using the pattern from TimeSeries
        // format the date using the pattern
        // parse it using DateUtils.
        // make sure it matches original date.
        ZoneId losAngeles = ZoneId.of("America/Los_Angeles");
        ZonedDateTime now = ZonedDateTime.of(2022, 12, 8, 9, 27, 14, 0, losAngeles);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TimeSeries.ZONED_DATE_TIME_FORMAT);  // FYI this pattern throws away milliseconds.
        String formatted = now.format(formatter);  // looks like: 2022-12-08T09:27:14-0800[America/Los_Angeles]

        ZonedDateTime parsed = DateUtils.parseUserDate(formatted, losAngeles, null);
        assertEquals(now, parsed, "Date parsed from TimeSeries format does not match original date");
    }

    @Test
    void test_bogus_input(){
        // Test that we get a DateTimeParseException when we pass in a bogus date.
        Assertions.assertThrows(DateTimeException.class, () -> DateUtils.parseUserDate("bogus", "America/Los_Angeles"));

        // What if it looks like it has a zone? this shouldn't parse
        Assertions.assertThrows(DateTimeException.class, () -> DateUtils.parseUserDate("bogus-07:00", "America/Los_Angeles"));


        // Test that we get a DateTimeException when we pass in a zone.
        Assertions.assertThrows(DateTimeException.class, () -> DateUtils.parseUserDate("2021-01-01T00:00:00Z", "garbage"));

    }

    @Test
    void test_has_time_zone(){
        assertTrue(DateUtils.hasZone("2021-01-01T00:00:00Z"), "Date with Z not matched");
        assertTrue(DateUtils.hasZone("2021-01-01T00:00:00-07:00"), "Date with offset not matched");
        assertTrue(DateUtils.hasZone("2021-01-01T00:00:00[US/Pacific]"), "Date with named timezone not matched");
        assertTrue(DateUtils.hasZone("2021-01-01T00:00:00[UTC]"), "Date with named timezone not matched");
        assertTrue(DateUtils.hasZone("2021-01-01T00:00:00[PST8PDT]"), "Date with named timezone not matched");
        assertTrue(DateUtils.hasZone("2021-01-01T00:00:00-0700"), "Date with offset not matched");
        assertTrue(DateUtils.hasZone("dontcareaboutthispart-07:00"), "offset not matched");
        assertTrue(DateUtils.hasZone("-07:00"), "offset not matched");
        assertTrue(DateUtils.hasZone("Z"), "offset not matched");
        assertFalse(DateUtils.hasZone("2021-01-01T00:00:00"), "Date without timezone matched");
    }

}
