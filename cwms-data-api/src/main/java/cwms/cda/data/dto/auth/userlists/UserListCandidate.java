package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class UserListCandidate {
    @JsonProperty(required = true)
    @Schema(description = "The existing CWMS user identifier.")
    private final String userId;

    @Schema(description = "The user's display name.")
    private final String fullName;

    @Schema(description = "The user's current email address.")
    private final String email;

    @Schema(description = "The user's current CWMS office identifier.")
    private final String officeId;

    /**
     * Creates a user-list membership candidate.
     */
    public UserListCandidate(String userId, String fullName, String email, String officeId) {
        this.userId = userId;
        this.fullName = fullName;
        this.email = email;
        this.officeId = officeId;
    }

    public String getUserId() {
        return userId;
    }

    public String getFullName() {
        return fullName;
    }

    public String getEmail() {
        return email;
    }

    public String getOfficeId() {
        return officeId;
    }
}
