package cwms.radar.helpers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class DateWithoutZoneArguments implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        ZoneId utc = ZoneId.of("UTC");
        ZoneId pacific = ZoneId.of("US/Pacific");
        ZoneId losAngeles = ZoneId.of("America/Los_Angeles");
        return Stream.of(
                Arguments.of("2022-01-06T00:00:01", pacific,
                        // hour=0, no tz in string but US/Pacific arg.  hour changed from 0->8
                        ZonedDateTime.of(2022, 1, 6, 8, 0, 1, 0, utc)),
                Arguments.of("2022-01-06T00:00:02", losAngeles,
                        // hour=0, no tz in string but US/Pacific arg.  hour changed from 0->8
                        ZonedDateTime.of(2022, 1, 6, 8, 0, 2, 0, utc)),
                Arguments.of("2022-01-06T00:00:03", utc,
                        // hour=0, no tz in string but utc arg.  hour not changed
                        ZonedDateTime.of(2022, 1, 6, 0, 0, 3, 0, utc)),
                Arguments.of("2021-04-05T00:00:04", pacific,
                        // hour=0, no tz in string but US/Pacific arg.  Hour changed from 0->7 (not 8 b/c of timechange)
                        ZonedDateTime.of(2021, 4, 5, 7, 0, 4, 0, utc))
        );

    }

}
