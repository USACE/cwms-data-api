package cwms.cda.data.dto.auth.users;

import java.util.List;
import java.util.Map;

import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.Formats;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import cwms.cda.data.dto.CwmsDTOBase;
import io.swagger.v3.oas.annotations.media.Schema;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class User extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final String userName;

    @Schema(description = "Unique identifier in the upstream identity management system.")
    @JsonProperty(required = true)
    private final String principal;

    public String getUserName() {
        return userName;
    }

    public String getPrincipal() {
        return this.principal;
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
    
    public User(String userName, String principal, String email, Map<String, List<String>> roles) {
        this.userName = userName;
        this.principal = principal;
        this.email = email;
        this.roles = roles;
    }

    @Override
    public String toString() {
        return "{" +
            " userName='" + getUserName() + "'" +
            ", principal='" + getPrincipal() + "'" +
            ", email='" + getEmail() + "'" +
            ", roles='" + getRoles() + "'" +
            "}";
    }
}
