package cwms.radar.helpers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class FullDatesArguments implements ArgumentsProvider{

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        return Stream.of(
            Arguments.of("2022-01-06T00:00:00Z",ZoneId.of("UTC"),ZonedDateTime.of(2022,1,6,0,0,0,0,ZoneId.of("UTC"))),
            Arguments.of("2022-01-06T07:00:00-07:00",ZoneId.of("UTC"),ZonedDateTime.of(2022,1,6,7,0,0,0,ZoneId.of("UTC"))),
            Arguments.of("2022-01-06T00:00:00",ZoneId.of("US/Pacific"),ZonedDateTime.of(2022,1,6,8,0,0,0,ZoneId.of("UTC"))),
            Arguments.of("2021-04-05T00:00:00",ZoneId.of("US/Pacific"),ZonedDateTime.of(2021,4,5,7,0,0,0,ZoneId.of("UTC")))
        );
    }

}
