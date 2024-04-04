package cwms.cda.data.dto.forecast;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;
import java.util.Map;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "forecast-instance")
@XmlAccessorType(XmlAccessType.FIELD)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = ForecastInstance.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ForecastInstance implements CwmsDTOBase {

    @Schema(description = "Forecast Spec")
    @XmlElement(name = "spec")
    private final ForecastSpec spec;

    @XmlAttribute(name = "date-time")
    private final Instant dateTime;

    @XmlAttribute(name = "issue-date-time")
    private final Instant issueDateTime;

    @XmlAttribute(name = "first-date-time")
    private final Instant firstDateTime;

    @XmlAttribute(name = "last-date-time")
    private final Instant lastDateTime;

    @XmlAttribute(name = "max-age")
    private final Integer maxAge;

    @XmlAttribute(name = "time-series-count")
    private final Integer timeSeriesCount;

    @Schema(description = "Forecast Instance Notes")
    @XmlAttribute
    private final String notes;

    @XmlAttribute(name = "metadata")
    private final Map<String, String> metadata;

    @Schema(description = "Forecast Filename")
    @XmlAttribute(name = "filename")
    private final String filename;

    @Schema(description = "Description of Forecast File")
    @XmlAttribute
    private final String fileDescription;

    @Schema(description = "Forecast File binary data")
    @XmlElement(name = "forecast-data")
    private final byte[] fileData;

    @Schema(description = "Link to Forecast File binary data")
    @XmlAttribute
    private final String fileDataUrl;


    private ForecastInstance(ForecastInstance.Builder builder) {

        this.spec = builder.spec;
        this.dateTime = builder.dateTime;
        this.issueDateTime = builder.issueDateTime;
        this.firstDateTime = builder.firstDateTime;
        this.lastDateTime = builder.lastDateTime;
        this.maxAge = builder.maxAge;
        this.timeSeriesCount = builder.timeSeriesCount;
        this.notes = builder.notes;
        this.metadata = builder.metadata;
        this.filename = builder.filename;
        this.fileDescription = builder.fileDescription;
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

    public Integer getTimeSeriesCount() {
        return timeSeriesCount;
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

    public byte[] getFileData() {
        return fileData;
    }

    public String getFileDataUrl() {
        return fileDataUrl;
    }

    public void validate() throws FieldException {
        //TODO
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
        private Integer timeSeriesCount;
        private String notes;
        private Map<String, String> metadata;
        private String filename;
        private String fileDescription;
        private byte[] fileData;
        private String fileDataUrl;

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

        public Builder withTimeSeriesCount(Integer timeSeriesCount) {
            this.timeSeriesCount = timeSeriesCount;
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
                    .withTimeSeriesCount(other.timeSeriesCount)
                    .withNotes(other.notes)
                    .withMetadata(other.metadata)
                    .withFilename(other.filename)
                    .withFileDescription(other.fileDescription)
                    .withFileData(other.fileData)
                    .withFileDataUrl(other.fileDataUrl)
                    ;
        }

        public ForecastInstance build() {
            return new ForecastInstance(this);
        }
    }

}
