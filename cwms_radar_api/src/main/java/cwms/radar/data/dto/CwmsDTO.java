package cwms.radar.data.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Base class for any DTO owned specifically by an office
 * to enforce consistency and the available to set the session
 * correctly.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public abstract class CwmsDTO implements CwmsDTOBase {
    @Schema(description = "Owning office of object.")
    @JsonProperty(required = true)
    private String officeId; // ALL DTOs require an office field

    protected CwmsDTO(String office) {
        this.officeId = office;
    }

    public String getOfficeId() {
        return officeId;
    }
}
