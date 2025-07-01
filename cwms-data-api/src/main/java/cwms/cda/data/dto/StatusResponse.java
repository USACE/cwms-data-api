package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

/**
 * Class for creating a JSON message for when there is a successful (non-error) response
 */
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class StatusResponse extends CwmsDTOBase{

    @JsonProperty(required = true)
    private String message;

    // Name or ID for identifying information
    private String identifier;

    // NOT FOR USE, only for Jackson deserialization
    public StatusResponse() {}

    public StatusResponse(String message) {
        this.message = message;
        this.identifier = "";
    }

    public StatusResponse(String message, String identifier) {
        this.message = message;
        this.identifier = identifier;
    }

    public String getMessage() {
        return message;
    }

    public String getIdentifier() {
        return identifier;
    }
}
