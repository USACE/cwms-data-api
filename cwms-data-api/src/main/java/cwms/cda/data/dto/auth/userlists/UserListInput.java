package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class UserListInput {
    @Schema(description = "The office that owns the list.")
    private String officeId;

    @Schema(description = "The office-scoped list identifier.", maxLength = 128,
        pattern = "[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
    private String userListId;

    @Schema(description = "Optional list description.", maxLength = 1024)
    private String description;

    public UserListInput() {
    }

    public String getOfficeId() {
        return officeId;
    }

    public void setOfficeId(String officeId) {
        this.officeId = officeId;
    }

    public String getUserListId() {
        return userListId;
    }

    public void setUserListId(String userListId) {
        this.userListId = userListId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
