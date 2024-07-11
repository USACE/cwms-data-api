package cwms.cda.data.dto.forecast;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.Map;

@JsonRootName("forecast-instance")
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = ForecastInstance.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ForecastInstance extends CwmsDTOBase {

    @Schema(description = "Forecast Spec")
    private final ForecastSpec spec;

    @JacksonXmlProperty(isAttribute = true)
    private final Instant dateTime;

    @JacksonXmlProperty(isAttribute = true)
    private final Instant issueDateTime;

    @JacksonXmlProperty(isAttribute = true)
    private final Instant firstDateTime;

    @JacksonXmlProperty(isAttribute = true)
    private final Instant lastDateTime;

    @JacksonXmlProperty(isAttribute = true)
    private final Integer maxAge;

    @Schema(description = "Forecast Instance Notes")
    @JacksonXmlProperty(isAttribute = true)
    private final String notes;

    @JacksonXmlProperty(isAttribute = true)
    private final Map<String, String> metadata;

    @Schema(description = "Forecast Filename")
    @JacksonXmlProperty(isAttribute = true)
    private final String filename;

    @Schema(description = "Description of Forecast File")
    @JacksonXmlProperty(isAttribute = true)
    private final String fileDescription;

    @Schema(description = "Forecast File Media Type")
    @JacksonXmlProperty(isAttribute = true)
    private final String fileMediaType;

    @Schema(description = "Forecast File binary data")
    private final byte[] fileData;

    @Schema(description = "Link to Forecast File binary data")
    @JacksonXmlProperty(isAttribute = true)
    private final String fileDataUrl;


    private ForecastInstance(ForecastInstance.Builder builder) {

        this.spec = builder.spec;
        this.dateTime = builder.dateTime;
        this.issueDateTime = builder.issueDateTime;
        this.firstDateTime = builder.firstDateTime;
        this.lastDateTime = builder.lastDateTime;
        this.maxAge = builder.maxAge;
        this.notes = builder.notes;
        this.metadata = builder.metadata;
        this.filename = builder.filename;
        this.fileDescription = builder.fileDescription;
        this.fileMediaType = builder.fileMediaType;
        this.fileData = builder.fileData;
        this.fileDataUrl = builder.fileDataUrl;
    }

    public ForecastSpec getSpec() {
        return spec;
    }


    public Instant getDateTime() {
        return dateTime;
    }

    public Instant getIssueDateTime() {
        return issueDateTime;
    }

    public Instant getFirstDateTime() {
        return firstDateTime;
    }

    public Instant getLastDateTime() {
        return lastDateTime;
    }

    public Integer getMaxAge() {
        return maxAge;
    }

    public String getNotes() {
        return notes;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public String getFilename() {
        return filename;
    }

    public String getFileDescription() {
        return fileDescription;
    }

    public String getFileMediaType() {
        return fileMediaType;
    }

    public byte[] getFileData() {
        return fileData;
    }

    public String getFileDataUrl() {
        return fileDataUrl;
    }

    @Override
    public String toString() {
        return "ForecastInstance{" +
                "spec=" + spec +
                ", dateTime=" + dateTime +
                ", issueDateTime=" + issueDateTime +
                ", firstDateTime=" + firstDateTime +
                ", lastDateTime=" + lastDateTime +
                ", maxAge=" + maxAge +
                ", notes='" + notes + '\'' +
                ", metadata=" + metadata +
                ", filename='" + filename + '\'' +
                ", fileDescription='" + fileDescription + '\'' +
                ", fileMediaType='" + fileMediaType + '\'' +
                ", fileData=" + fileData +
                ", fileDataUrl='" + fileDataUrl + '\'' +
                '}';
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private ForecastSpec spec;
        private Instant dateTime;
        private Instant issueDateTime;
        private Instant firstDateTime;
        private Instant lastDateTime;
        private Integer maxAge;
        private String notes;
        private Map<String, String> metadata;
        private String filename;
        private String fileDescription;
        private byte[] fileData;
        private String fileDataUrl;
        private String fileMediaType;

        public Builder() {
        }


        public Builder withSpec(ForecastSpec spec) {
            this.spec = spec;
            return this;
        }


        public Builder withDateTime(Instant dateTime) {
            this.dateTime = dateTime;
            return this;
        }

        public Builder withIssueDateTime(Instant issueDateTime) {
            this.issueDateTime = issueDateTime;
            return this;
        }

        public Builder withFirstDateTime(Instant firstDateTime) {
            this.firstDateTime = firstDateTime;
            return this;
        }

        public Builder withLastDateTime(Instant lastDateTime) {
            this.lastDateTime = lastDateTime;
            return this;
        }

        public Builder withMaxAge(Integer maxAge) {
            this.maxAge = maxAge;
            return this;
        }

        public Builder withNotes(String notes) {
            this.notes = notes;
            return this;
        }

        public Builder withMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder withFilename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder withFileDescription(String fileDescription) {
            this.fileDescription = fileDescription;
            return this;
        }

        public Builder withFileMediaType(String fileMediaType) {
            this.fileMediaType = fileMediaType;
            return this;
        }

        public Builder withFileData(byte[] fileData) {
            this.fileData = fileData;
            return this;
        }

        public Builder withFileDataUrl(String fileDataUrl) {
            this.fileDataUrl = fileDataUrl;
            return this;
        }

        public Builder from(ForecastInstance other) {
            ForecastSpec otherSpec = other.spec;
            if(otherSpec != null) {
                otherSpec = new ForecastSpec.Builder().from(otherSpec).build();
            }
            return withSpec(otherSpec)
                    .withDateTime(other.dateTime)
                    .withIssueDateTime(other.issueDateTime)
                    .withFirstDateTime(other.firstDateTime)
                    .withLastDateTime(other.lastDateTime)
                    .withMaxAge(other.maxAge)
                    .withNotes(other.notes)
                    .withMetadata(other.metadata)
                    .withFilename(other.filename)
                    .withFileDescription(other.fileDescription)
                    .withFileMediaType(other.fileMediaType)
                    .withFileData(other.fileData)
                    .withFileDataUrl(other.fileDataUrl)
                    ;
        }

        public ForecastInstance build() {
            return new ForecastInstance(this);
        }
    }

}
