package cwms.cda.helpers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class FullDatesArguments implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        // It doesn't matter what this is because all these test cases include a timezone.
        ZoneId unusedZone = ZoneId.of("Europe/Paris");

        // This is the zone used for generating the expected results.
        // The test uses ZonedDateTime.isEqual() to compare the expected and actual results.
        ZoneId utc = ZoneId.of("UTC");

        return Stream.of(
                Arguments.of("2022-01-06T00:00:00Z", unusedZone,
                        // hour=0, Has Z and UTC so hour should match
                        ZonedDateTime.of(2022, 1, 6, 0, 0, 0, 0, utc)),
                Arguments.of("2022-01-06T07:00:01-07:00", unusedZone,
                        // hour=7, offset of -7 so UTC hour should be 14
                        ZonedDateTime.of(2022, 1, 6, 14, 0, 1, 0, utc)),
                Arguments.of("2022-01-06T07:00:02-0700", unusedZone,
                        // hour=7, offset of -7 so UTC hour should be 14
                        ZonedDateTime.of(2022, 1, 6, 14, 0, 2, 0, utc)),
                Arguments.of("2019-09-07T15:50+00:00", unusedZone,
                        // hour=15, 0 offset so its already utc and hour should match
                        ZonedDateTime.of(2019, 9, 7, 15, 50, 0, 0, utc)),
                Arguments.of("2021-06-10T13:00:23-07:00[PST8PDT]", unusedZone,
                        // hour=13, -7 offset.  hour should be 20
                        ZonedDateTime.of(2021, 6, 10, 20, 0, 23, 0, utc)),
                Arguments.of("2022-01-19T20:52:07+00:00[UTC]", unusedZone,
                        // hour=20, 0 offset, hour should match
                        ZonedDateTime.of(2022, 1, 19, 20, 52, 7, 0, utc)),
                Arguments.of("2022-01-19T20:52:53+0000[UTC]", unusedZone,
                        // Just like above but with a 4 digit offset that doesn't have a colon
                        ZonedDateTime.of(2022, 1, 19, 20, 52, 53, 0, utc)),
                Arguments.of("2021-06-10T13:00:00-0700[PST8PDT]", unusedZone,
                        // This is the example that was in the TimeSeriesController documentation. No colon in offset.
                        // hour=13, -7 offset, hour should be 20
                        ZonedDateTime.of(2021, 6, 10, 20, 0, 0, 0, utc)),
                Arguments.of("2022-12-08T09:47:32-0800[America/Los_Angeles]", unusedZone,
                        // This is what the TimeSeries.ZONED_DATE_TIME_FORMAT produces
                        // hour=9, -8 offset, hour should be 17
                        ZonedDateTime.of(2022, 12, 8, 17, 47, 32, 0, utc)),
                Arguments.of("2022-12-08T09:47:32-0800[America/Los_Angeles]", unusedZone,
                        // Repeat of above but comparing to an expected result that isn't utc just to
                        // verify that the test doesn't require the expected result to be in utc.
                        // hour=9, -8 offset, but comparing to ZDT in Los Angeles, so hour should be 9
                        ZonedDateTime.of(2022, 12, 8, 9, 47, 32, 0, ZoneId.of("America/Los_Angeles")))
        );

    }

}
