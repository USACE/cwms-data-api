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

    private final Long attribute;
    private final String mediaType;
    private final String fileExtension;
    private final byte[] binaryValue;


    private BinaryTimeSeriesRow(Builder builder) {
        this.dateTime = builder.dateTime;

        this.dataEntryDate = builder.dataEntryDate;

        this.attribute = builder.attribute;
        this.mediaType = builder.mediaType;
        this.fileExtension = builder.fileExtension;
        this.binaryValue = builder.binaryValue;
    }

    public Instant getDateTime() {
        return dateTime;
    }



    public Instant getDataEntryDate() {
        return dataEntryDate;
    }



    public Long getAttribute() {
        return attribute;
    }

    public String getMediaType() {
        return mediaType;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public byte[] getBinaryValue() {
        return binaryValue;
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

        if (getAttribute() != null ? !getAttribute().equals(that.getAttribute()) : that.getAttribute() != null)
            return false;
        if (getMediaType() != null ? !getMediaType().equals(that.getMediaType()) : that.getMediaType() != null)
            return false;
        if (getFileExtension() != null ? !getFileExtension().equals(that.getFileExtension()) : that.getFileExtension() != null)
            return false;
        return Arrays.equals(getBinaryValue(), that.getBinaryValue());
    }

    @Override
    public int hashCode() {
        int result = getDateTime() != null ? getDateTime().hashCode() : 0;
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
        result = 31 * result + (getMediaType() != null ? getMediaType().hashCode() : 0);
        result = 31 * result + (getFileExtension() != null ? getFileExtension().hashCode() : 0);
        result = 31 * result + Arrays.hashCode(getBinaryValue());
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {

        private Instant dateTime;
        private Instant dataEntryDate;
        private Long attribute;
        private String mediaType;
        private String fileExtension;
        private byte[] binaryValue;

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


        public Builder withAttribute(Long attribute){
            this.attribute = attribute;
            return this;
        }

        public Builder withMediaType(String mediaType){
            this.mediaType = mediaType;
            return this;
        }

        public Builder withFileExtension(String fileExtension){
            this.fileExtension = fileExtension;
            return this;
        }

        public Builder withBinaryValue(byte[] binaryData){
            this.binaryValue = binaryData;
            return this;
        }

        public Builder from(BinaryTimeSeriesRow row){
            if(row == null){
                return withDateTime(null)
                        .withDataEntryDate(null)
                        .withBinaryValue(null)
                        .withFileExtension(null)
                        .withMediaType(null)
                        .withAttribute(null);
            } else {
                return withDateTime(row.dateTime)
                        .withDataEntryDate(row.dataEntryDate)
                        .withBinaryValue(row.binaryValue)
                        .withFileExtension(row.fileExtension)
                        .withMediaType(row.mediaType)
                        .withAttribute(row.attribute);
            }
        }

        public BinaryTimeSeriesRow build(){
            return new BinaryTimeSeriesRow(this);
        }
    }

}
