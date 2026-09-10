package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class UserListMemberInput {
    @Schema(description = "An existing CWMS user identifier.", maxLength = 128)
    private String userId;

    public UserListMemberInput() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
