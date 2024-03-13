package cwms.cda.data.dto.texttimeseries;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import hec.data.timeSeriesText.DateDateKey;
import java.time.Instant;
import java.util.Date;

@JsonDeserialize(builder = RegularTextTimeSeriesRow.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RegularTextTimeSeriesRow implements TextTimeSeriesRow {

    private final Instant dateTime;
    private final Instant dataEntryDate;
    private final String textValue;
    private final String filename;
    private final String mediaType;
    private final Long qualityCode;
    private final int destFlag;
    private final String valueUrl;



    private RegularTextTimeSeriesRow(Builder builder) {
        dateTime = builder.dateTime;
        dataEntryDate = builder.dataEntryDate;
        textValue = builder.textValue;
        filename = builder.filename;
        mediaType = builder.mediaType;
        qualityCode = builder.qualityCode;
        destFlag = builder.destFlag;
        valueUrl = builder.valueUrl;
    }



    public String getTextValue() {
        return textValue;
    }

    public String getFilename() {
        return filename;
    }

    public String getMediaType() {
        return mediaType;
    }

    public Long getQualityCode() {
        return qualityCode;
    }

    public int getDestFlag() {
        return destFlag;
    }

    public String getValueUrl() {
        return valueUrl;
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegularTextTimeSeriesRow that = (RegularTextTimeSeriesRow) o;

        if (getDestFlag() != that.getDestFlag()) return false;
        if (getDateTime() != null ? !getDateTime().equals(that.getDateTime()) : that.getDateTime() != null)
            return false;
        if (getDataEntryDate() != null ? !getDataEntryDate().equals(that.getDataEntryDate()) : that.getDataEntryDate() != null)
            return false;
        if (getTextValue() != null ? !getTextValue().equals(that.getTextValue()) : that.getTextValue() != null)
            return false;
        if (getFilename() != null ? !getFilename().equals(that.getFilename()) : that.getFilename() != null)
            return false;
        if (getMediaType() != null ? !getMediaType().equals(that.getMediaType()) : that.getMediaType() != null)
            return false;
        return getQualityCode() != null ? getQualityCode().equals(that.getQualityCode()) : that.getQualityCode() == null;
    }

    @Override
    public int hashCode() {
        int result = getDateTime() != null ? getDateTime().hashCode() : 0;
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getTextValue() != null ? getTextValue().hashCode() : 0);
        result = 31 * result + (getFilename() != null ? getFilename().hashCode() : 0);
        result = 31 * result + (getMediaType() != null ? getMediaType().hashCode() : 0);
        result = 31 * result + (getQualityCode() != null ? getQualityCode().hashCode() : 0);
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private Instant dateTime;
        private Instant dataEntryDate;
        private String textValue;
        private String filename;
        private String mediaType;
        private Long qualityCode;
        private int destFlag;
        private String valueUrl;


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

        public Builder withTextValue(String textValue) {
            this.textValue = textValue;
            return this;
        }

        public Builder withFilename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder withMediaType(String mediaType) {
            this.mediaType = mediaType;
            return this;
        }

        public Builder withQualityCode(Long qualityCode) {
            this.qualityCode = qualityCode;
            return this;
        }

        public Builder withDestFlag(int destFlag) {
            this.destFlag = destFlag;
            return this;
        }

        public Builder withValueUrl(String valueUrl) {
            this.valueUrl = valueUrl;
            return this;
        }

        public RegularTextTimeSeriesRow build() {
            return new RegularTextTimeSeriesRow(this);
        }

        public Builder from(RegularTextTimeSeriesRow regularTextTimeSeriesRow) {
            if (regularTextTimeSeriesRow == null) {
                return withDateTime(null)
                        .withDataEntryDate(null)
                        .withTextValue(null)
                        .withFilename(null)
                        .withMediaType(null)
                        .withQualityCode(null)
                        .withDestFlag(0)
                        .withValueUrl(null)
                        ;
            } else {
                return withDateTime(regularTextTimeSeriesRow.dateTime)
                        .withDataEntryDate(regularTextTimeSeriesRow.dataEntryDate)
                        .withTextValue(regularTextTimeSeriesRow.textValue)
                        .withFilename(regularTextTimeSeriesRow.filename)
                        .withMediaType(regularTextTimeSeriesRow.mediaType)
                        .withQualityCode(regularTextTimeSeriesRow.qualityCode)
                        .withDestFlag(regularTextTimeSeriesRow.destFlag)
                        .withValueUrl(regularTextTimeSeriesRow.valueUrl)
                        ;
            }
        }
    }
}
