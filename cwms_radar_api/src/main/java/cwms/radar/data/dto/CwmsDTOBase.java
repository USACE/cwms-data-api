package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import cwms.radar.api.errors.FieldException;
/*
 * Suitable base for objects not owned by an office, like a list of locations.
 * Or parameters, or the list of offices themselves.
 */
public interface CwmsDTOBase {
    public  void validate() throws FieldException; // additional validation logic.
}
