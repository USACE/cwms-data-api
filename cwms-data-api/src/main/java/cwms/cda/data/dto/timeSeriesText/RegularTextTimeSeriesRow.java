package cwms.cda.data.dto.timeSeriesText;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import hec.data.timeSeriesText.DateDateKey;
import hec.data.timeSeriesText.TextTimeSeriesRow;
import java.util.Date;

@JsonDeserialize(builder = RegularTextTimeSeriesRow.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RegularTextTimeSeriesRow implements TextTimeSeriesRow {
    private final Date _dateTime;
    private final Date _versionDate;
    private final Date _dataEntryDate;
    private final String _textId;
    private final Long _attribute;
    private final String _textValue;
    private final boolean _newData;

    private RegularTextTimeSeriesRow(Builder builder) {
        _dateTime = builder._dateTime;
        _versionDate = builder._versionDate;
        _dataEntryDate = builder._dataEntryDate;
        _textId = builder._textId;
        _attribute = builder._attribute;
        _textValue = builder._textValue;
        _newData = builder._newData;
    }

    @Override
    public RegularTextTimeSeriesRow copy() {
        return new RegularTextTimeSeriesRow.Builder().from(this).build();
    }

    @Override
    public Date getDateTime() {
        return null;
    }

    @Override
    public Date getDataEntryDate() {
        return null;
    }

    @Override
    public DateDateKey getDateDateKey() {
        return null;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private Date _dateTime;
        private Date _versionDate;
        private Date _dataEntryDate;
        private String _textId;
        private Long _attribute;
        private String _textValue;
        private boolean _newData;

        public Builder() {
        }

        public Builder withDateTime(Date dateTime) {
            _dateTime = dateTime;
            return this;
        }

        public Builder withVersionDate(Date versionDate) {
            _versionDate = versionDate;
            return this;
        }

        public Builder withDataEntryDate(Date dataEntryDate) {
            _dataEntryDate = dataEntryDate;
            return this;
        }

        public Builder withTextId(String textId) {
            _textId = textId;
            return this;
        }

        public Builder withAttribute(Long attribute) {
            _attribute = attribute;
            return this;
        }

        public Builder withTextValue(String textValue) {
            _textValue = textValue;
            return this;
        }

        public Builder withNewData(boolean newData) {
            _newData = newData;
            return this;
        }

        public RegularTextTimeSeriesRow build() {
            return new RegularTextTimeSeriesRow(this);
        }

        public Builder from(RegularTextTimeSeriesRow regularTextTimeSeriesRow) {
            if(regularTextTimeSeriesRow == null){
                return withDateTime(null)
                        .withVersionDate(null)
                        .withDataEntryDate(null)
                        .withTextId(null)
                        .withAttribute(null)
                        .withTextValue(null)
                        .withNewData(false);
            } else {
                return withDateTime(regularTextTimeSeriesRow._dateTime)
                        .withVersionDate(regularTextTimeSeriesRow._versionDate)
                        .withDataEntryDate(regularTextTimeSeriesRow._dataEntryDate)
                        .withTextId(regularTextTimeSeriesRow._textId)
                        .withAttribute(regularTextTimeSeriesRow._attribute)
                        .withTextValue(regularTextTimeSeriesRow._textValue)
                        .withNewData(regularTextTimeSeriesRow._newData);
            }
        }
    }
}
