package cwms.radar.data.dto;

import cwms.radar.api.errors.FieldException;

public interface CwmsDTO {
    public void validate() throws FieldException; // additional validation logic.
}
