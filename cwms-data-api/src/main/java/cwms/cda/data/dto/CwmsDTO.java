package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Base class for any DTO owned specifically by an office
 * to enforce consistency and the available to set the session
 * correctly.
 */
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public abstract class CwmsDTO extends CwmsDTOBase {
    @Schema(description = "Owning office of object.")
    @JsonProperty(required=true)
    protected final String officeId; // ALL DTOs require an office field

    protected CwmsDTO(String office) {
        this.officeId = office;
    }

    public String getOfficeId() {
        return officeId;
    }
}
