package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.errors.FieldException;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Base class for any DTO owned specifically by an office
 * to enforce consistency and the available to set the session
 * correctly.
 */
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public abstract class CwmsDTO implements CwmsDTOBase {
    @Schema(description = "Owning office of object.")
    @JsonProperty(required = true)
    protected final String officeId; // ALL DTOs require an office field

    protected CwmsDTO(String office) {
        this.officeId = office;
    }

    public String getOfficeId() {
        return officeId;
    }

    @Override
    public final void validate() throws FieldException {
        CwmsDTOValidator validator = new CwmsDTOValidator();
        validateInternal(validator);
        validator.validate();
    }

    protected void validateInternal(CwmsDTOValidator validator) {
        //No-op for compatibility
        //Eventually make validate() final and this method abstract
    }
}
