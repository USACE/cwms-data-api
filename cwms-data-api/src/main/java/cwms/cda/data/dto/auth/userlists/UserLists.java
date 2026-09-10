package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.Collections;
import java.util.List;

@JsonRootName("user-lists")
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class,
    aliases = {Formats.DEFAULT, Formats.JSON})
public final class UserLists extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final List<UserList> userLists;

    public UserLists(List<UserList> userLists) {
        this.userLists = List.copyOf(userLists);
    }

    public List<UserList> getUserLists() {
        return Collections.unmodifiableList(userLists);
    }
}
