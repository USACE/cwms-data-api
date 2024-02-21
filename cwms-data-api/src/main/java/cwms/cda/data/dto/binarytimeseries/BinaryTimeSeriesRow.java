package cwms.cda.data.dto.binarytimeseries;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Arrays;
import java.util.Date;

@JsonDeserialize(builder = BinaryTimeSeriesRow.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class BinaryTimeSeriesRow {
    private final Date dateTime;
    private final Date versionDate;
    private final Date dataEntryDate;
    private final String binaryId;
    private final Long attribute;
    private final String mediaType;
    private final String fileExtension;
    private final byte[] binaryValue;


    private BinaryTimeSeriesRow(Builder builder) {
        this.dateTime = builder.dateTime;
        this.versionDate = builder.versionDate;
        this.dataEntryDate = builder.dataEntryDate;
        this.binaryId = builder.binaryId;
        this.attribute = builder.attribute;
        this.mediaType = builder.mediaType;
        this.fileExtension = builder.fileExtension;
        this.binaryValue = builder.binaryValue;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public Date getVersionDate() {
        return versionDate;
    }

    public Date getDataEntryDate() {
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
        if (getVersionDate() != null ? !getVersionDate().equals(that.getVersionDate()) : that.getVersionDate() != null)
            return false;
        if (getDataEntryDate() != null ? !getDataEntryDate().equals(that.getDataEntryDate()) : that.getDataEntryDate() != null)
            return false;
        if (getBinaryId() != null ? !getBinaryId().equals(that.getBinaryId()) : that.getBinaryId() != null)
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
        result = 31 * result + (getVersionDate() != null ? getVersionDate().hashCode() : 0);
        result = 31 * result + (getDataEntryDate() != null ? getDataEntryDate().hashCode() : 0);
        result = 31 * result + (getBinaryId() != null ? getBinaryId().hashCode() : 0);
        result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
        result = 31 * result + (getMediaType() != null ? getMediaType().hashCode() : 0);
        result = 31 * result + (getFileExtension() != null ? getFileExtension().hashCode() : 0);
        result = 31 * result + Arrays.hashCode(getBinaryValue());
        return result;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {

        private Date dateTime;
        private  Date versionDate;
        private  Date dataEntryDate;
        private  String binaryId;
        private  Long attribute;
        private  String mediaType;
        private  String fileExtension;
        private  byte[] binaryValue;

        public Builder(){

        }

        public Builder withDateTime(Date dateTime){
            this.dateTime = dateTime;
            return this;
        }

        public Builder withVersionDate(Date versionDate){
            this.versionDate = versionDate;
            return this;
        }

        public Builder withDataEntryDate(Date dataEntryDate){
            this.dataEntryDate = dataEntryDate;
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

        public Builder from(BinaryTimeSeriesRow row){
            if(row == null){
                return withDateTime(null)
                        .withDataEntryDate(null)
                        .withVersionDate(null)
                        .withBinaryId(null)
                        .withBinaryValue(null)
                        .withFileExtension(null)
                        .withMediaType(null)
                        .withAttribute(null);
            } else {
                return withDateTime(row.dateTime)
                        .withDataEntryDate(row.dataEntryDate)
                        .withVersionDate(row.versionDate)
                        .withBinaryId(row.binaryId)
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
