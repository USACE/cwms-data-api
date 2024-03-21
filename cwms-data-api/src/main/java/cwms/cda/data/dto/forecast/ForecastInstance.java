package cwms.cda.data.dto.forecast;

import cwms.cda.api.errors.FieldException;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.*;
import java.time.Instant;
import java.util.Map;

@XmlRootElement(name = "forecast-instance")
@XmlAccessorType(XmlAccessType.FIELD)
public class ForecastInstance {

  // unique identifier: specId + issueDateTime

  @Schema(description = "Forecast Spec ID")
  @XmlElement(name = "spec-id")
  private String specId;

  @XmlAttribute(name = "date-time")
  private Instant dateTime;

  @XmlAttribute(name = "issue-date-time")
  private Instant issueDateTime;

  @XmlAttribute(name = "first-date-time")
  private Instant firstDateTime;

  @XmlAttribute(name = "last-date-time")
  private Instant lastDateTime;

  @XmlAttribute(name = "max-age")
  private Integer maxAge;

  @XmlAttribute(name = "time-series-count")
  private Integer timeSeriesCount;

  @Schema(description = "Forecast Instance Notes")
  @XmlAttribute
  private String notes;

  @XmlAttribute(name = "metadata")
  private Map<String, String> metadata;

  @Schema(description = "Forecast Filename")
  @XmlAttribute(name = "filename")
  private String filename;

  @Schema(description = "Description of Forecast File")
  @XmlAttribute
  private String fileDescription;

  @Schema(description = "Forecast File binary data")
  @XmlElement(name = "forecast-data")
  private byte[] fileData;

  @SuppressWarnings("unused") // required so JAXB can initialize and marshal
  private ForecastInstance() {}

  public ForecastInstance(String specId, Instant dateTime, Instant issueDateTime,
                          Instant firstDateTime, Instant lastDateTime, Integer maxAge, Integer timeSeriesCount,
                          String notes, Map<String, String> metadata, String filename, String fileDescription,
                          byte[] fileData) {
    this.specId = specId;
    this.dateTime = dateTime;
    this.issueDateTime = issueDateTime;
    this.firstDateTime = firstDateTime;
    this.lastDateTime = lastDateTime;
    this.maxAge = maxAge;
    this.timeSeriesCount = timeSeriesCount;
    this.notes = notes;
    this.metadata = metadata;
    this.filename = filename;
    this.fileDescription = fileDescription;
    this.fileData = fileData;
  }

  public String getSpecId() {
    return specId;
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

  public void validate() throws FieldException {
    //TODO
  }

}
