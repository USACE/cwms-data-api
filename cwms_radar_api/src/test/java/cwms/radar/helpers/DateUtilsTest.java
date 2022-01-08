package cwms.radar.helpers;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class DateUtilsTest {


    @ParameterizedTest
    @ArgumentsSource(FullDatesArguments.class)
    public void test_iso_dates_from_user( String inputDate, ZoneId tz, ZonedDateTime expected){
        ZonedDateTime result = DateUtils.parseUserDate(inputDate, tz,null); // now not used in this case force npe if
                                                                            // someone accidentally sets that up.
        assertTrue(result.isEqual(expected), "Provided date input not correctly matched");
    }


    @ParameterizedTest
    @ArgumentsSource(PeriodArguments.class)
    public void test_iso_period_from_user(ZonedDateTime now, String inputPeriod, ZoneId tz, ZonedDateTime expected){
        ZonedDateTime result = DateUtils.parseUserDate(inputPeriod, tz, now);
        assertTrue(result.isEqual(expected), "User provided input not correctly matched to expected result");
    }

    @ParameterizedTest
    @ArgumentsSource(DurationArguments.class)
    public void test_iso_duration_from_user(ZonedDateTime now, String inputPeriod, ZoneId tz, ZonedDateTime expected){
        ZonedDateTime result = DateUtils.parseUserDate(inputPeriod, tz, now);
        assertTrue(result.isEqual(expected), "User provided input not correctly matched to expected result");
    }

}
