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
    private final Instant versionDate;
    private final Instant dataEntryDate;
    private final String binaryId;
    private final Long attribute;
    private final String mediaType;
    private final String fileExtension;
    private final byte[] binaryValue;

    private String url;


    private BinaryTimeSeriesRow(Builder builder) {
        this.dateTime = builder.dateTime;
        this.versionDate = builder.versionDate;
        this.dataEntryDate = builder.dataEntryDate;
        this.binaryId = builder.binaryId;
        this.attribute = builder.attribute;
        this.mediaType = builder.mediaType;
        this.fileExtension = builder.fileExtension;
        this.binaryValue = builder.binaryValue;
        this.url = builder.url;
    }

    public Instant getDateTime() {
        return dateTime;
    }

    public Instant getVersionDate() {
        return versionDate;
    }

    public Instant getDataEntryDate() {
        return dataEntryDate;
    }

    public String getBinaryId() {
        return binaryId;
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

    public String getUrl() {
        return url;
    }

    public BinaryTimeSeriesRow copy(){
        return new Builder().from(this).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinaryTimeSeriesRow row = (BinaryTimeSeriesRow) o;

        if (getDateTime() != null ? !getDateTime().equals(row.getDateTime()) : row.getDateTime() != null)
            return false;
        if (getVersionDate() != null ? !getVersionDate().equals(row.getVersionDate()) : row.getVersionDate() != null)
            return false;
        if (getDataEntryDate() != null ? !getDataEntryDate().equals(row.getDataEntryDate()) : row.getDataEntryDate() != null)
            return false;
        if (getBinaryId() != null ? !getBinaryId().equals(row.getBinaryId()) : row.getBinaryId() != null)
            return false;
        if (getAttribute() != null ? !getAttribute().equals(row.getAttribute()) : row.getAttribute() != null)
            return false;
        if (getMediaType() != null ? !getMediaType().equals(row.getMediaType()) : row.getMediaType() != null)
            return false;
        return getFileExtension() != null ? getFileExtension().equals(row.getFileExtension()) : row.getFileExtension() == null;
    }

    @Override
    public int hashCode() {
        int result = getDateTime() != null ? getDateTime().hashCode() : 0;
        result = 31 * result + (getVersionDate() != null ? getVersionDate().hashCode() : 0);
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getBinaryId() != null ? getBinaryId().hashCode() : 0);
        result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
        result = 31 * result + (getMediaType() != null ? getMediaType().hashCode() : 0);
        result = 31 * result + (getFileExtension() != null ? getFileExtension().hashCode() : 0);
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {

        private Instant dateTime;
        private Instant versionDate;
        private Instant dataEntryDate;
        private String binaryId;
        private Long attribute;
        private String mediaType;
        private String fileExtension;
        private byte[] binaryValue;

        private String url;

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

        public Builder withVersionDate(Instant versionDate){
            this.versionDate = versionDate;
            return this;
        }

        public Builder withVersionDate(long versionDateEpochMillis){
            this.versionDate = Instant.ofEpochMilli(versionDateEpochMillis);
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

        public Builder withBinaryId(String binaryId){
            this.binaryId = binaryId;
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

        public Builder withUrl(String url){
            this.url = url;
            return this;
        }

        public Builder from(BinaryTimeSeriesRow row){
            if(row == null){
                return withDateTime(null)
                        .withDataEntryDate(null)
                        .withVersionDate(null)
                        .withBinaryId(null)
                        .withBinaryValue(null)
                        .withFileExtension(null)
                        .withMediaType(null)
                        .withAttribute(null)
                        .withUrl(null)
                        ;
            } else {
                return withDateTime(row.dateTime)
                        .withDataEntryDate(row.dataEntryDate)
                        .withVersionDate(row.versionDate)
                        .withBinaryId(row.binaryId)
                        .withBinaryValue(row.binaryValue)
                        .withFileExtension(row.fileExtension)
                        .withMediaType(row.mediaType)
                        .withAttribute(row.attribute)
                        .withUrl(row.url)
                        ;
            }
        }

        public BinaryTimeSeriesRow build(){
            return new BinaryTimeSeriesRow(this);
        }
    }

}
