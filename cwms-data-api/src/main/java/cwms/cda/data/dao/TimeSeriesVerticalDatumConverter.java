package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.VerticalDatumInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class TimeSeriesVerticalDatumConverter {


    private TimeSeriesVerticalDatumConverter() {
        throw new AssertionError("Utility class, don't instantiate");
    }

    public static TimeSeries convertToVerticalDatum(TimeSeries originalTimeSeries, VerticalDatum convertTo) {
        VerticalDatum vd = getVerticalDatum(originalTimeSeries).orElse(convertTo);
        if(Objects.equals(convertTo, vd)) {
            return originalTimeSeries; //no conversion needed
        }
        TimeSeries retVal = originalTimeSeries;
        VerticalDatumInfo vdi = originalTimeSeries.getVerticalDatumInfo();
        VerticalDatumInfo.Offset offset = vdi.getOffsetForDatum(convertTo);
        if(offset != null)
        {
            List<TimeSeries.Record> newValues = applyOffsetToValues(offset.getValue(), originalTimeSeries.getValues());
            VerticalDatumInfo newVerticalDatumInfo = vdi.convertedTo(offset);
            retVal = new TimeSeries(originalTimeSeries.getPage(),
                    originalTimeSeries.getPageSize(),
                    originalTimeSeries.getTotal(),
                    originalTimeSeries.getName(),
                    originalTimeSeries.getOfficeId(),
                    originalTimeSeries.getBegin(),
                    originalTimeSeries.getEnd(),
                    originalTimeSeries.getUnits(),
                    originalTimeSeries.getInterval(),
                    newVerticalDatumInfo,
                    originalTimeSeries.getIntervalOffset(),
                    originalTimeSeries.getTimeZone(),
                    originalTimeSeries.getVersionDate(),
                    originalTimeSeries.getDateVersionType())
                    .withValues(newValues);
        }
        return retVal;
    }

    @NotNull
    private static List<TimeSeries.Record> applyOffsetToValues(Double offset, List<TimeSeries.Record> originalValues) {
        List<TimeSeries.Record> newValues = new ArrayList<>();
        for (TimeSeries.Record record : originalValues) {
            Double newValue = record.getValue() + offset;
            TimeSeries.Record newRecord = new TimeSeries.Record(record.getDateTime(), newValue, record.getQualityCode());
            newValues.add(newRecord);
        }
        return newValues;
    }

    public static Optional<VerticalDatum> getVerticalDatum(TimeSeries timeSeries) {
        return Optional.ofNullable(timeSeries)
                .map(TimeSeries::getVerticalDatumInfo)
                .map(VerticalDatumInfo::getNativeDatum)
                .filter(s -> !s.isEmpty())
                .map(s -> {
                    if (s.equalsIgnoreCase(VerticalDatum.OTHER.toString())) {
                        throw new IllegalArgumentException("Vertical Datum of OTHER is not currently supported.");
                    }
                    return VerticalDatum.getVerticalDatum(s);
                });
    }

}
