package cwms.cda.data.dto.texttimeseries;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import hec.data.timeSeriesText.DateDateKey;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;

@JsonDeserialize(builder = RegularTextTimeSeriesRow.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RegularTextTimeSeriesRow implements TextTimeSeriesRow {

    private final Instant dateTime;
    private final Instant dataEntryDate;
    private final Long attribute;
    private final String textValue;


    private RegularTextTimeSeriesRow(Builder builder) {
        dateTime = builder.dateTime;
        dataEntryDate = builder.dataEntryDate;
        attribute = builder.attribute;
        textValue = builder.textValue;
    }

    public Long getAttribute() {
        return attribute;
    }

    public String getTextValue() {
        return textValue;
    }


    public RegularTextTimeSeriesRow copy() {
        return new RegularTextTimeSeriesRow.Builder().from(this).build();
    }


    public Instant getDateTime() {
        return dateTime;
    }


    public Instant getDataEntryDate() {
        return dataEntryDate;
    }

    @JsonIgnore
    public DateDateKey getDateDateKey() {
        return new DateDateKey(dateTime == null? null: Date.from(dateTime), dataEntryDate==null ? null: Date.from(dataEntryDate));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegularTextTimeSeriesRow that = (RegularTextTimeSeriesRow) o;


        if (getDateTime() != null ? !getDateTime().equals(that.getDateTime()) :
                that.getDateTime() != null) {
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
        return getTextValue() != null ? getTextValue().equals(that.getTextValue()) :
                that.getTextValue() == null;
    }

    @Override
    public int hashCode() {
        int result = getDateTime() != null ? getDateTime().hashCode() : 0;
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
        result = 31 * result + (getTextValue() != null ? getTextValue().hashCode() : 0);

        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private Instant dateTime;
        private Instant dataEntryDate;
        private Long attribute;
        private String textValue;


        public Builder() {
        }

        public Builder withDateTime(Instant dateTime) {
            this.dateTime = dateTime;
            return this;
        }

        public Builder withDataEntryDate(Instant dataEntryDate) {
            this.dataEntryDate = dataEntryDate;
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

        public Builder withAttribute(BigDecimal attribute) {
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

        public RegularTextTimeSeriesRow build() {
            return new RegularTextTimeSeriesRow(this);
        }

        public Builder from(RegularTextTimeSeriesRow regularTextTimeSeriesRow) {
            if (regularTextTimeSeriesRow == null) {
                return withDateTime(null)
                        .withDataEntryDate(null)
                        .withAttribute((Long) null)
                        .withTextValue(null)
                        ;
            } else {
                return withDateTime(regularTextTimeSeriesRow.dateTime)
                        .withDataEntryDate(regularTextTimeSeriesRow.dataEntryDate)
                        .withAttribute(regularTextTimeSeriesRow.attribute)
                        .withTextValue(regularTextTimeSeriesRow.textValue)
                        ;
            }
        }
    }
}
