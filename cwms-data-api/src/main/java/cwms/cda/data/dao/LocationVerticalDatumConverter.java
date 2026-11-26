package cwms.cda.data.dao;

import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.VerticalDatumInfo;

import java.util.Objects;
import java.util.Optional;

public final class LocationVerticalDatumConverter {

    private LocationVerticalDatumConverter() {
        throw new AssertionError("Utility class, don't instantiate");
    }

    public static Location convertToVerticalDatum(Location originalLocation, VerticalDatum convertTo, VerticalDatumInfo vdi) {
        if (originalLocation == null || convertTo == null) {
            return originalLocation;
        }
        if (vdi == null) {
            return originalLocation;
        }
        VerticalDatum current = getVerticalDatum(originalLocation).orElse(convertTo);
        if (Objects.equals(convertTo, current)) {
            return originalLocation;
        }
        VerticalDatumInfo.Offset offset = vdi.getOffsetForDatum(convertTo);
        if (offset == null) {
            return originalLocation;
        }
        VerticalDatumInfo newVdi = vdi.convertedTo(offset);
        return new Location.Builder(originalLocation)
                .withElevation(newVdi.getElevation())
                .withElevationUnits(newVdi.getUnit())
                .withVerticalDatum(newVdi.getNativeDatum())
                .build();
    }

    public static Optional<VerticalDatum> getVerticalDatum(Location location) {
        return Optional.ofNullable(location)
                .map(Location::getVerticalDatum) // unwrap Optional<VerticalDatumInfo>
                .filter(s -> !s.isBlank())
                .map(VerticalDatum::getVerticalDatum);
    }
}
