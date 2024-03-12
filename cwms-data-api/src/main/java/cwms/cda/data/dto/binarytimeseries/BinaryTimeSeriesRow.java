package cwms.cda.data.dto.binarytimeseries;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.time.Instant;
import java.util.Arrays;

@JsonDeserialize(builder = BinaryTimeSeriesRow.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class BinaryTimeSeriesRow {
    private final Instant dateTime;
    private final Instant dataEntryDate;
    private final String mediaType;
    private final String filename;
    private final int destFlag;
    private final byte[] binaryValue;
    private final String valueUrl;


    private BinaryTimeSeriesRow(Builder builder) {
        this.dateTime = builder.dateTime;
        this.dataEntryDate = builder.dataEntryDate;
        this.mediaType = builder.mediaType;
        this.filename = builder.filename;
        this.binaryValue = builder.binaryValue;
        this.destFlag = builder.destFlag;
        this.valueUrl = builder.valueUrl;
    }

    public Instant getDateTime() {
        return dateTime;
    }

    public Instant getDataEntryDate() {
        return dataEntryDate;
    }

    public String getMediaType() {
        return mediaType;
    }

    public String getFilename() {
        return filename;
    }

    public byte[] getBinaryValue() {
        return binaryValue;
    }

    public int getDestFlag() {
        return destFlag;
    }

    public String getValueUrl() {
        return valueUrl;
    }

    public BinaryTimeSeriesRow copy(){
        return new Builder().from(this).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinaryTimeSeriesRow that = (BinaryTimeSeriesRow) o;

        if (getDateTime() != null ? !getDateTime().equals(that.getDateTime()) : that.getDateTime() != null)
            return false;

        if (getDataEntryDate() != null ? !getDataEntryDate().equals(that.getDataEntryDate()) : that.getDataEntryDate() != null)
            return false;

        if (getMediaType() != null ? !getMediaType().equals(that.getMediaType()) : that.getMediaType() != null)
            return false;
        if (getFilename() != null ? !getFilename().equals(that.getFilename()) : that.getFilename() != null)
            return false;
        return Arrays.equals(getBinaryValue(), that.getBinaryValue());
    }

    @Override
    public int hashCode() {
        int result = getDateTime() != null ? getDateTime().hashCode() : 0;
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getMediaType() != null ? getMediaType().hashCode() : 0);
        result = 31 * result + (getFilename() != null ? getFilename().hashCode() : 0);
        result = 31 * result + Arrays.hashCode(getBinaryValue());
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {

        private Instant dateTime;
        private Instant dataEntryDate;
        private String mediaType;
        private String filename;
        private String valueUrl;
        private byte[] binaryValue;
        public int destFlag = 0;

        public Builder(){

        }

        public Builder withDateTime(Instant dateTime){
            this.dateTime = dateTime;
            return this;
        }


        public Builder withDateTime(long dateTimeEpochMillis){
            this.dateTime = Instant.ofEpochMilli(dateTimeEpochMillis);
            return this;
        }


        public Builder withDataEntryDate(Instant dataEntryDate){
            this.dataEntryDate = dataEntryDate;
            return this;
        }

        public Builder withDataEntryDate(long dataEntryDateEpochMillis){
            this.dataEntryDate = Instant.ofEpochMilli(dataEntryDateEpochMillis);
            return this;
        }


        public Builder withMediaType(String mediaType){
            this.mediaType = mediaType;
            return this;
        }

        public Builder withFilename(String filename){
            this.filename = filename;
            return this;
        }

        public Builder withBinaryValue(byte[] binaryData){
            this.binaryValue = binaryData;
            return this;
        }

        public Builder withDestFlag(int destFlag){
            this.destFlag = destFlag;
            return this;
        }

        public Builder withValueUrl(String valueUrl){
            this.valueUrl = valueUrl;
            return this;
        }

        public Builder from(BinaryTimeSeriesRow row){
            if(row == null){
                return withDateTime(null)
                        .withDataEntryDate(null)
                        .withBinaryValue(null)
                        .withFilename(null)
                        .withMediaType(null)
                        .withDestFlag(0)
                        .withValueUrl(null)
                        ;
            } else {
                return withDateTime(row.dateTime)
                        .withDataEntryDate(row.dataEntryDate)
                        .withBinaryValue(row.binaryValue)
                        .withFilename(row.filename)
                        .withMediaType(row.mediaType)
                        .withDestFlag(row.destFlag)
                        .withValueUrl(this.valueUrl)
                        ;
            }
        }

        public BinaryTimeSeriesRow build(){
            return new BinaryTimeSeriesRow(this);
        }
    }

}
