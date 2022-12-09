package cwms.radar.helpers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class FullDatesArguments implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        ZoneId utc = ZoneId.of("UTC");
        ZoneId pacific = ZoneId.of("US/Pacific");
        ZoneId losAngeles = ZoneId.of("America/Los_Angeles");
        return Stream.of(
                Arguments.of("2022-01-06T00:00:00Z", utc,
                        ZonedDateTime.of(2022, 1, 6, 0, 0, 0, 0, utc)),
                Arguments.of("2022-01-06T07:00:01-07:00", utc,
                        ZonedDateTime.of(2022, 1, 6, 14, 0, 1, 0, utc)),
                Arguments.of("2022-01-06T07:00:02-0700", utc,
                        ZonedDateTime.of(2022, 1, 6, 14, 0, 2, 0, utc)),
                Arguments.of("2022-01-06T00:00:03", pacific,
                        // no tz in string but US/Pacific arg.  Note hour changed from 0->8
                        ZonedDateTime.of(2022, 1, 6, 8, 0, 3, 0, utc)),
                Arguments.of("2021-04-05T00:00:04", pacific,
                        // no tz in string but US/Pacific arg.  Hour changed from 0->7 b/c of timechange
                        ZonedDateTime.of(2021, 4, 5, 7, 0, 4, 0, utc)),
                Arguments.of("2019-09-07T15:50+00:00", utc,
                        // no tz but there is an offset.
                        ZonedDateTime.of(2019, 9, 7, 15, 50, 0, 0, utc)),
                Arguments.of("2021-06-10T13:00:23-07:00[PST8PDT]", utc,
                        // Has a bracket zoneId, which I think isn't strictly ISO but java likes it.
                        ZonedDateTime.of(2021, 6, 10, 20, 0, 23, 0, utc)),
                Arguments.of("2022-01-19T20:52:07+00:00[UTC]", utc,
                        // Has a bracket zoneId of [UTC]
                        ZonedDateTime.of(2022, 1, 19, 20, 52, 7, 0, utc)),
                Arguments.of("2022-01-19T20:52:53+0000[UTC]", utc,
                        // Just like above but with a 4 digit offset that doesn't have a colon
                        ZonedDateTime.of(2022, 1, 19, 20, 52, 53, 0, utc)),
                Arguments.of("2021-06-10T13:00:00-0700[PST8PDT]", utc,
                        // The example that was in the TimeSeriesController documentation. No colon in offset.
                        ZonedDateTime.of(2021, 6, 10, 20, 0, 0, 0, utc)),
                Arguments.of("2022-12-08T09:47:32-0800[America/Los_Angeles]", losAngeles,
                        // This is what the TimeSeries.ZONED_DATE_TIME_FORMAT produces
                        ZonedDateTime.of(2022, 12, 8, 9, 47, 32, 0, losAngeles))
        );

    }

}
