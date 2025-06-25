package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;


@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"office-id", "response", "identifier"})
public final class StatusResponse extends CwmsDTO{

    @JsonProperty(required = true)
    private final String response;

    private final String identifier;

    public StatusResponse(String officeId, String response) {
        super(officeId);
        this.response = response;
        // or should I change to null? Only occurs when there is no identifier
        this.identifier = "";
    }

    @JsonCreator
    public StatusResponse(@JsonProperty("office-id") String officeId, @JsonProperty("response") String response, @JsonProperty("identifier") String identifier) {
        super(officeId);
        this.response = response;
        this.identifier = identifier;
    }

    public String getResponse() {
        return response;
    }

    public String getIdentifier() {
        return identifier;
    }
}
