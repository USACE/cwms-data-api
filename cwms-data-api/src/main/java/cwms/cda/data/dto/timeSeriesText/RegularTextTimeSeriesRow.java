package cwms.cda.data.dto.timeSeriesText;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    private final Date dateTime;
    private final Date versionDate;
    private final Date dataEntryDate;
    private final String textId;
    private final Long attribute;
    private final String textValue;
    private final boolean newData;

    private RegularTextTimeSeriesRow(Builder builder) {
        dateTime = builder.dateTime;
        versionDate = builder.versionDate;
        dataEntryDate = builder.dataEntryDate;
        textId = builder.textId;
        attribute = builder.attribute;
        textValue = builder.textValue;
        newData = builder.newData;
    }

    public Date getVersionDate() {
        return versionDate;
    }

    public String getTextId() {
        return textId;
    }

    public Long getAttribute() {
        return attribute;
    }

    public String getTextValue() {
        return textValue;
    }

    public boolean isNewData() {
        return newData;
    }

    @Override
    public RegularTextTimeSeriesRow copy() {
        return new RegularTextTimeSeriesRow.Builder().from(this).build();
    }

    @Override
    public Date getDateTime() {
        return dateTime;
    }

    @Override
    public Date getDataEntryDate() {
        return dataEntryDate;
    }

    @JsonIgnore
    @Override
    public DateDateKey getDateDateKey() {
        return new DateDateKey(dateTime, dataEntryDate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegularTextTimeSeriesRow that = (RegularTextTimeSeriesRow) o;

        if (isNewData() != that.isNewData()) return false;
        if (getDateTime() != null ? !getDateTime().equals(that.getDateTime()) :
                that.getDateTime() != null)
            return false;
        if (getVersionDate() != null ? !getVersionDate().equals(that.getVersionDate()) :
                that.getVersionDate() != null)
            return false;
        if (getDataEntryDate() != null ? !getDataEntryDate().equals(that.getDataEntryDate()) :
                that.getDataEntryDate() != null)
            return false;
        if (getTextId() != null ? !getTextId().equals(that.getTextId()) : that.getTextId() != null)
            return false;
        if (getAttribute() != null ? !getAttribute().equals(that.getAttribute()) :
                that.getAttribute() != null)
            return false;
        return getTextValue() != null ? getTextValue().equals(that.getTextValue()) :
                that.getTextValue() == null;
    }

    @Override
    public int hashCode() {
        int result = getDateTime() != null ? getDateTime().hashCode() : 0;
        result = 31 * result + (getVersionDate() != null ? getVersionDate().hashCode() : 0);
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getTextId() != null ? getTextId().hashCode() : 0);
        result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
        result = 31 * result + (getTextValue() != null ? getTextValue().hashCode() : 0);
        result = 31 * result + (isNewData() ? 1 : 0);
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private Date dateTime;
        private Date versionDate;
        private Date dataEntryDate;
        private String textId;
        private Long attribute;
        private String textValue;
        private boolean newData;

        public Builder() {
        }

        public Builder withDateTime(Date dateTime) {
            this.dateTime = dateTime;
            return this;
        }

        public Builder withVersionDate(Date versionDate) {
            this.versionDate = versionDate;
            return this;
        }

        public Builder withDataEntryDate(Date dataEntryDate) {
            this.dataEntryDate = dataEntryDate;
            return this;
        }

        public Builder withTextId(String textId) {
            this.textId = textId;
            return this;
        }

        @JsonProperty("attribute")
        public Builder withAttribute(Long attribute) {
            this.attribute = attribute;
            return this;
        }


        public Builder withAttribute(Integer attribute) {
            if (attribute == null) {
                this.attribute = null;
            } else {
                this.attribute = attribute.longValue();
            }
            return this;
        }

        public Builder withTextValue(String textValue) {
            this.textValue = textValue;
            return this;
        }

        public Builder withNewData(boolean newData) {
            this.newData = newData;
            return this;
        }

        public RegularTextTimeSeriesRow build() {
            return new RegularTextTimeSeriesRow(this);
        }

        public Builder from(RegularTextTimeSeriesRow regularTextTimeSeriesRow) {
            if (regularTextTimeSeriesRow == null) {
                return withDateTime(null)
                        .withVersionDate(null)
                        .withDataEntryDate(null)
                        .withTextId(null)
                        .withAttribute((Long) null)
                        .withTextValue(null)
                        .withNewData(false);
            } else {
                return withDateTime(regularTextTimeSeriesRow.dateTime)
                        .withVersionDate(regularTextTimeSeriesRow.versionDate)
                        .withDataEntryDate(regularTextTimeSeriesRow.dataEntryDate)
                        .withTextId(regularTextTimeSeriesRow.textId)
                        .withAttribute(regularTextTimeSeriesRow.attribute)
                        .withTextValue(regularTextTimeSeriesRow.textValue)
                        .withNewData(regularTextTimeSeriesRow.newData);
            }
        }
    }
}
