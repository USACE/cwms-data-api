package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.VerticalDatumInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TimeSeriesVerticalDatumConverter {


    private TimeSeriesVerticalDatumConverter() {
        throw new AssertionError("Utility class, don't instantiate");
    }

    public static TimeSeries convertToVerticalDatum(TimeSeries originalTimeSeries, VerticalDatum convertTo) {
        if(originalTimeSeries.getVerticalDatumInfo() == null || Objects.equals(convertTo, getVerticalDatum(originalTimeSeries))) {
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

    private static VerticalDatum getVerticalDatum(@Nullable TimeSeries timeSeries) {

        VerticalDatum retVal = null;
        if (timeSeries != null) {
            VerticalDatumInfo vdi = timeSeries.getVerticalDatumInfo();
            if (vdi != null) {
                String nativeDatum = vdi.getNativeDatum();
                if (nativeDatum != null && !nativeDatum.isEmpty()) {
                    if (nativeDatum.equalsIgnoreCase("OTHER")) {
                        throw new IllegalArgumentException("Vertical Datum of OTHER is not currently supported.");
                    } else {
                        retVal = VerticalDatum.getVerticalDatum(nativeDatum);
                    }
                }
            }
        }

        return retVal;
    }

}
