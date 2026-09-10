package cwms.cda.data.dto.auth.userlists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class UserListCandidates {
    @JsonProperty(required = true)
    @Schema(description = "Existing CWMS users matching the supplied search text.")
    private final List<UserListCandidate> candidates;

    public UserListCandidates(List<UserListCandidate> candidates) {
        this.candidates = candidates;
    }

    public List<UserListCandidate> getCandidates() {
        return candidates;
    }
}
