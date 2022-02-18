package cwms.radar.helpers;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class DurationArguments implements ArgumentsProvider{

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        // Test argument order ZonedDateTime now, String inputPeriod, ZoneId tz, ZonedDateTime expected
        ZoneId utc = ZoneId.of("UTC");
        ZoneId pacific = ZoneId.of("US/Pacific");
        ZonedDateTime nowNormalDay = ZonedDateTime.of(2022,1,6,0,0,0,0,utc);
        ZonedDateTime nowDstBoundaryMarch = ZonedDateTime.of(2022,3,14,0,0,0,0,pacific);
        ZonedDateTime nowDstBoundaryNov = ZonedDateTime.of(2022,11,6,0,0,0,0,pacific);

        return Stream.of(
            Arguments.of(nowNormalDay,"P-1D","UTC",ZonedDateTime.of(2022,1,5,0,0,0,0,utc)),
            Arguments.of(nowDstBoundaryMarch,"P-1D","US/Pacific",ZonedDateTime.of(2022,3,13,8,0,0,0,utc)),
            Arguments.of(nowDstBoundaryNov,"P-1D","US/Pacific",ZonedDateTime.of(2022,11,5,7,0,0,0,utc))
        );
    }

}
