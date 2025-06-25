package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class StatusResponse extends CwmsDTO{

    @JsonProperty("response-message")
    private final String response;
    private final String identifier;

    public StatusResponse(String officeId, String response) {
        super(officeId);
        this.response = response;
        // or should I change to null? Only occurs when there is no identifier
        this.identifier = "";
    }

    public StatusResponse(String officeId, String response, String identifier) {
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
