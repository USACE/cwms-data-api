package cwms.cda.data.dto.texttimeseries;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import hec.data.timeSeriesText.DateDateKey;

import hec.data.timeSeriesText.TextTimeSeriesRow;
import java.math.BigDecimal;
import java.util.Date;

@JsonDeserialize(builder = StandardTextTimeSeriesRow.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class StandardTextTimeSeriesRow implements TextTimeSeriesRow {

    private final Date dateTime;
    private final Date versionDate;
    private final Date dataEntryDate;
    private final Long attribute;
    private final String officeId;
    private final String standardTextId;
    private final String textValue;
    private final String url;


    private StandardTextTimeSeriesRow(Builder builder) {
        this.officeId = builder.officeId;
        this.standardTextId = builder.standardTextId;
        this.textValue = builder.textValue;
        this.dateTime = builder.dateTime;
        this.versionDate = builder.versionDate;
        this.dataEntryDate = builder.dataEntryDate;
        this.attribute = builder.attribute;
        this.url = builder.url;
    }



    @Override
    public <Y extends TextTimeSeriesRow> Y copy() {
        return null;
    }

    @Override
    public Date getDateTime() {
        return dateTime;
    }

    @Override
    public Date getDataEntryDate() {
        return dataEntryDate;
    }

    public Date getVersionDate() {
        return versionDate;
    }

    public Long getAttribute() {
        return attribute;
    }

    @JsonIgnore
    @Override
    public DateDateKey getDateDateKey() {
        return new DateDateKey(dateTime, dataEntryDate);
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getStandardTextId() {
        return standardTextId;
    }

    public String getTextValue() {
        return textValue;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StandardTextTimeSeriesRow that = (StandardTextTimeSeriesRow) o;

        if (getDateTime() != null ? !getDateTime().equals(that.getDateTime()) :
                that.getDateTime() != null) {
            return false;
        }
        if (getVersionDate() != null ? !getVersionDate().equals(that.getVersionDate()) :
                that.getVersionDate() != null) {
            return false;
        }
        if (getDataEntryDate() != null ? !getDataEntryDate().equals(that.getDataEntryDate()) :
                that.getDataEntryDate() != null) {
            return false;
        }
        if (getAttribute() != null ? !getAttribute().equals(that.getAttribute()) :
                that.getAttribute() != null) {
            return false;
        }
        if (getOfficeId() != null ? !getOfficeId().equals(that.getOfficeId()) :
                that.getOfficeId() != null) {
            return false;
        }
        if (getStandardTextId() != null ? !getStandardTextId().equals(that.getStandardTextId()) :
                that.getStandardTextId() != null) {
            return false;
        }
        return getTextValue() != null ? getTextValue().equals(that.getTextValue()) :
                that.getTextValue() == null;
    }

    @Override
    public int hashCode() {
        int result = getDateTime() != null ? getDateTime().hashCode() : 0;
        result = 31 * result + (getVersionDate() != null ? getVersionDate().hashCode() : 0);
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
        result = 31 * result + (getOfficeId() != null ? getOfficeId().hashCode() : 0);
        result = 31 * result + (getStandardTextId() != null ? getStandardTextId().hashCode() : 0);
        result = 31 * result + (getTextValue() != null ? getTextValue().hashCode() : 0);
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private Date dateTime;
        private Date versionDate;
        private Date dataEntryDate;
        private String standardTextId;
        private Long attribute;
        private String textValue;

        private String officeId;

        private String url;

        public Builder() {
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withTextValue(String textValue) {
            this.textValue = textValue;
            return this;
        }

        public Builder withStandardTextId(String stdTextId) {
            this.standardTextId = stdTextId;
            return this;
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


        @JsonProperty("attribute")
        public Builder withAttribute(Long l) {
            this.attribute = l;
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

        public Builder withAttribute(BigDecimal attribute) {
            if (attribute == null) {
                this.attribute = null;
            } else {
                this.attribute = attribute.longValue();
            }
            return this;
        }

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }


        public StandardTextTimeSeriesRow.Builder from(StandardTextTimeSeriesRow stdRow) {
            if (stdRow == null) {
                return withDateTime(null)
                        .withVersionDate(null)
                        .withDataEntryDate(null)
                        .withAttribute((Long) null)
                        .withStandardTextId(null)
                        .withTextValue(null)
                        .withOfficeId(null)
                        .withUrl(null)
                        ;
            } else {
                return withDateTime(stdRow.dateTime)
                        .withVersionDate(stdRow.versionDate)
                        .withDataEntryDate(stdRow.dataEntryDate)
                        .withAttribute(stdRow.attribute)
                        .withStandardTextId(stdRow.standardTextId)
                        .withTextValue(stdRow.textValue)
                        .withOfficeId(stdRow.officeId)
                        .withUrl(stdRow.url)
                        ;
            }
        }

        public StandardTextTimeSeriesRow build() {
            return new StandardTextTimeSeriesRow(this);
        }

    }
}
