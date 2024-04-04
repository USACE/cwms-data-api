package cwms.cda.data.dto.forecast;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "forecast-spec")
@XmlAccessorType(XmlAccessType.FIELD)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class ForecastSpec extends CwmsDTO {
    @Schema(description = "Forecast Spec ID")
    @XmlElement(name = "spec-id")
    private String specId;

    @Schema(description = "Location ID")
    @XmlElement(name = "location-id")
    private String locationId;

    @Schema(description = "Source Entity ID")
    @XmlElement(name = "source-entity-id")
    private String sourceEntityId;

    @Schema(description = "Forecast Designator")
    @XmlAttribute
    private String designator;

    @Schema(description = "Description of Forecast")
    @XmlAttribute
    private String description;

    @Schema(description = "List of Time Series IDs belonging to this Forecast Spec")
    @XmlAttribute(name = "time-series-ids")
    private List<TimeSeriesIdentifierDescriptor> timeSeriesIds;

    @SuppressWarnings("unused") // required so JAXB can initialize and marshal
    private ForecastSpec() {
        super(null);
    }

    public ForecastSpec(String specId, String officeId, String locationId, String sourceEntityId,
                        String designator,
                        String description, List<TimeSeriesIdentifierDescriptor> timeSeriesIds) {
        super(officeId);
        this.specId = specId;
        this.locationId = locationId;
        this.sourceEntityId = sourceEntityId;
        this.designator = designator;
        this.description = description;
        this.timeSeriesIds = timeSeriesIds;
    }

    public String getSpecId() {
        return specId;
    }

    public String getLocationId() {
        return locationId;
    }

    public String getSourceEntityId() {
        return sourceEntityId;
    }

    public String getDesignator() {
        return designator;
    }

    public String getDescription() {
        return description;
    }

    public List<TimeSeriesIdentifierDescriptor> getTimeSeriesIds() {
        return timeSeriesIds;
    }

    public void validate() throws FieldException {
        //TODO
    }

}
