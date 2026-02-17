package cwms.cda.data.dao;

import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.VerticalDatumInfo;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

public class LocationVerticalDatumConverterTest {

    @Test
    void testConvertVerticalDatumOnLocation() {
        VerticalDatumInfo.Offset[] offsets = new VerticalDatumInfo.Offset[] {
                new VerticalDatumInfo.Offset(false, "NGVD-29", 0.0),
                new VerticalDatumInfo.Offset(false, "NAVD-88", -0.5)
        };
        VerticalDatumInfo vdi = new VerticalDatumInfo.Builder()
                .withOffice("LRL")
                .withLocation("TEST_LOCATION")
                .withUnit("m")
                .withNativeDatum("NGVD-29")
                .withElevation(100.0)
                .withOffsets(offsets)
                .build();

        Location loc = new Location.Builder("TEST_LOCATION", "SITE", ZoneId.of("UTC"),
                50.0, 50.0, "NGVD29", "LRL")
                .withElevationUnits("m")
                .withVerticalDatum("NGVD-29")
                .withElevation(100.0)
                .build();

        Location converted = LocationVerticalDatumConverter.convertToVerticalDatum(loc, VerticalDatum.NAVD88, vdi);

        assertEquals("NAVD-88", converted.getVerticalDatum());
        assertEquals(99.5, converted.getElevation(), 1e-6);

        // round-trip back to NGVD29
        Location back = LocationVerticalDatumConverter.convertToVerticalDatum(converted, VerticalDatum.NGVD29, vdi);
        assertEquals("NGVD-29", back.getVerticalDatum());
        assertEquals(100.0, back.getElevation(), 1e-6);
    }
}
