package cwms.cda.data.dto.auth;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import io.swagger.v3.oas.annotations.media.Schema;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonInclude(value = Include.NON_NULL)
@Schema(description = "Timeseries group privilege with embargo information")
public class TsGroupPrivilege {

    @JsonProperty(required = true)
    @Schema(description = "Numeric code identifying the timeseries group")
    private final Integer tsGroupCode;

    @JsonProperty(required = true)
    @Schema(description = "Human-readable identifier for the timeseries group")
    private final String tsGroupId;

    @Schema(description = "User privilege level for this group: read, write, or read-write")
    private final String privilege;

    @Schema(description = "Number of hours data in this group is embargoed. 0 means no embargo.")
    private final Integer embargoHours;

    public TsGroupPrivilege(Integer tsGroupCode, String tsGroupId, String privilege, Integer embargoHours) {
        this.tsGroupCode = tsGroupCode;
        this.tsGroupId = tsGroupId;
        this.privilege = privilege;
        this.embargoHours = embargoHours;
    }

    public Integer getTsGroupCode() {
        return tsGroupCode;
    }

    public String getTsGroupId() {
        return tsGroupId;
    }

    public String getPrivilege() {
        return privilege;
    }

    public Integer getEmbargoHours() {
        return embargoHours;
    }

    @Override
    public String toString() {
        return "{" +
            " tsGroupCode=" + tsGroupCode +
            ", tsGroupId='" + tsGroupId + "'" +
            ", privilege='" + privilege + "'" +
            ", embargoHours=" + embargoHours +
            "}";
    }
}
