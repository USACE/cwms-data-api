package cwms.cda.data.dto.auth.users;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.Formats;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import cwms.cda.data.dto.CwmsDTOBase;
import io.swagger.v3.oas.annotations.media.Schema;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonInclude(value = Include.NON_NULL)
@JsonDeserialize(builder = User.Builder.class)
public class User extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final String userName;

    @Schema(description = "Unique identifier in the upstream identity management system.")
    @JsonProperty(required = true)
    private final String principal;

    @Schema(description = "Is the current session based on CAC Authentication")
    @JsonProperty(access = Access.READ_ONLY)
    private final Boolean cacAuth;

    public String getUserName() {
        return userName;
    }

    public String getPrincipal() {
        return this.principal;
    }

    public Boolean getCacAuth() {
        return cacAuth;
    }


    public String getEmail() {
        return this.email;
    }


    public Map<String,List<String>> getRoles() {
        return this.roles;
    }


    @JsonProperty(required = true)
    @Schema(format = "email")
    private final String email;


    @Schema(description = "Assigned user roles per office.")
    private final Map<String,List<String>> roles;

    public User(String userName, String principal, String email, Boolean cac_auth, Map<String, List<String>> roles) {
        this.userName = userName;
        this.principal = principal;
        this.email = email;
        this.roles = roles;
        this.cacAuth = cac_auth;
    }

    @Override
    public String toString() {
        return "{" +
            " userName='" + getUserName() + "'" +
            ", principal='" + getPrincipal() + "'" +
            ", email='" + getEmail() + "'" +
            ", usedCac='" + getCacAuth() + "'" +
            ", roles='" + getRoles() + "'" +
            "}";
    }


    public static class Builder {
        private final User tmp;

        @JsonCreator
        public Builder(@JsonProperty("user-name") String userName,
                       @JsonProperty("principal") String principal,
                       @JsonProperty("email") String email,
                       @JsonProperty("cac-auth") Boolean cac_auth) {
            tmp = new User(userName, principal, email, cac_auth, new HashMap<>());
        }

        public Builder addRole(String office, String role) {
            tmp.roles.computeIfAbsent(office, (key) -> new ArrayList<>()).add(role);

            return this;
        }

        @JsonSetter("roles")
        public Builder addRoles(Map<String, List<String>> roles) {
            tmp.roles.putAll(roles);
            return this;
        }

        public User build() {
            return tmp;
        }
    }
}
