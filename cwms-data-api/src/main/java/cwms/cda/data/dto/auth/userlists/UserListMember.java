package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
    aliases = {Formats.DEFAULT, Formats.JSON})
public final class UserListMember extends CwmsDTOBase {

    @JsonProperty(required = true)
    @Schema(description = "The owning CWMS office identifier for the user list.")
    private final String officeId;

    @JsonProperty(required = true)
    @Schema(description = "The office-scoped identifier of the user list.", maxLength = 128)
    private final String userListId;

    @JsonProperty(required = true)
    @Schema(description = "The user identifier for the member.", maxLength = 128)
    private final String userId;

    @Schema(description = "The user's display name.")
    private final String fullName;

    @Schema(description = "The user's email address.")
    private final String email;

    /**
     * Creates an office-scoped user-list member representation.
     */
    public UserListMember(String officeId, String userListId, String userId, String fullName,
            String email) {
        this.officeId = officeId;
        this.userListId = userListId;
        this.userId = userId;
        this.fullName = fullName;
        this.email = email;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getUserListId() {
        return userListId;
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
}
