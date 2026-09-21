package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.List;

@JsonRootName("user-list-members")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
    aliases = {Formats.DEFAULT, Formats.JSON})
public final class UserListMembers extends CwmsDTOBase {

    @JsonProperty(required = true)
    @Schema(description = "Members in the requested user list.")
    private final List<UserListMember> members;

    public UserListMembers(List<UserListMember> members) {
        this.members = List.copyOf(members);
    }

    public List<UserListMember> getMembers() {
        return Collections.unmodifiableList(members);
    }
}
