package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import hec.data.ITimeSeriesDescription;
import hec.data.timeSeriesText.DateDateComparator;
import hec.data.timeSeriesText.DateDateKey;
import hec.data.timeSeriesText.TextTimeSeriesRow;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TextTimeSeries<T extends TextTimeSeriesRow> extends CwmsDTOPaginated {
    private String description;
    private String id;
    private String officeId;

    NavigableMap<DateDateKey, T> _textTimeSeriesMap = new TreeMap<>(new DateDateComparator());

    public TextTimeSeries() {
        super(null);
        id = null;
        description = null;
    }

    public TextTimeSeries(String id, String officeId, String description) {
        super(officeId);
        this.id = id;
        this.description = description;
    }

    public TextTimeSeries(ITimeSeriesDescription timeSeriesDescription) {
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public String getOfficeId() {
        return officeId;
    }


    @Override
    public void validate() throws FieldException {

    }

    public T add(T timeSeriesRow)
    {
        T retval = null;
        DateDateKey dateDateKey = timeSeriesRow.getDateDateKey();
        retval = _textTimeSeriesMap.put(dateDateKey, timeSeriesRow);
        return retval;
    }


    public NavigableMap<DateDateKey,T> getTextTimeSeriesMap() {
        return _textTimeSeriesMap;
    }
}
