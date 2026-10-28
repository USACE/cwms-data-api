package cwms.cda.data.dto;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import cwms.cda.api.errors.FieldException;
import io.javalin.openapi.OpenApiNaming;
import io.javalin.openapi.OpenApiNamingStrategy;
/*
 * Suitable base for objects not owned by an office, like a list of locations.
 * Or parameters, or the list of offices themselves.
 */
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@OpenApiNaming(OpenApiNamingStrategy.KEBAB_CASE)
public abstract class CwmsDTOBase {

    public final void validate() throws FieldException {
        CwmsDTOValidator validator = new CwmsDTOValidator();
        validateInternal(validator);
        validator.validate();
    }

    /**
     * This method can be overridden to provide additional validation logic
     * @param validator validator that will aggregate all validation errors
     */
    protected void validateInternal(CwmsDTOValidator validator) {
        validator.validateRequiredFields(this);
    }
}
