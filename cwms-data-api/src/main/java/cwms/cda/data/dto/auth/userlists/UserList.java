package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
    aliases = {Formats.DEFAULT, Formats.JSON})
public final class UserList extends CwmsDTOBase {

    @JsonProperty(required = true)
    @Schema(description = "The owning CWMS office identifier for the user list.")
    private final String officeId;

    @JsonProperty(required = true)
    @Schema(description = "The office-scoped identifier of the user list.", maxLength = 128)
    private final String userListId;

    @Schema(description = "The user list description.", maxLength = 1024)
    private final String description;

    @Schema(description = "The immutable user id of the authenticated principal that created "
            + "the list.")
    private final String ownedByUserId;

    @JsonProperty(required = true)
    @Schema(description = "The time the user list was created.")
    private final Instant createdAt;

    @Schema(description = "The time the user list was last updated.")
    private final Instant updatedAt;

    /**
     * Creates an office-scoped user-list representation.
     */
    public UserList(String officeId, String userListId, String description, String ownedByUserId,
            Instant createdAt, Instant updatedAt) {
        this.officeId = officeId;
        this.userListId = userListId;
        this.description = description;
        this.ownedByUserId = ownedByUserId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getUserListId() {
        return userListId;
    }

    public String getDescription() {
        return description;
    }

    public String getOwnedByUserId() {
        return ownedByUserId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
