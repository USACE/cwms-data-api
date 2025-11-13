package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.VerticalDatumInfo;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class TimeSeriesVerticalDatumConverter {


    private TimeSeriesVerticalDatumConverter() {
        throw new AssertionError("Utility class, don't instantiate");
    }

    public static TimeSeries convertToVerticalDatum(TimeSeries timeSeries, VerticalDatum convertTo) {
        if(timeSeries.getVerticalDatumInfo() == null || Objects.equals(convertTo, getVerticalDatum(timeSeries))) {
            return timeSeries; //no conversion needed
        }
        TimeSeries retVal = new TimeSeries(timeSeries);
        VerticalDatumInfo vdi = timeSeries.getVerticalDatumInfo();
        VerticalDatumInfo.Offset[] offsets = vdi.getOffsets();
        for (VerticalDatumInfo.Offset offset : offsets) {
            String toDatum = offset.getToDatum();
            if (toDatum.replaceAll("-", "").equalsIgnoreCase(convertTo.name())) {
                Double conversionFactor = offset.getValue();
                List<TimeSeries.Record> values = retVal.getValues();
                List<TimeSeries.Record> newValues = new ArrayList<>();
                for (TimeSeries.Record record : values) {
                    Double newValue = record.getValue() + conversionFactor;
                    TimeSeries.Record newRecord = new TimeSeries.Record(record.getDateTime(), newValue, record.getQualityCode());
                    newValues.add(newRecord);
                }
                values.clear();
                values.addAll(newValues);
            }
        }
        return retVal;
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
