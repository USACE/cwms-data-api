package cwms.cda.data.dto.auth;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ApiKey {
    @JsonProperty(required = true)
    private String userId;
    @JsonProperty(required = true)
    private String keyName;
    
    private String apiKey;
    private Date created;
    private Date expires;

    public ApiKey(String userId, String keyName) {
        this.userId = userId;
        this.keyName = keyName;
    }

    public ApiKey(String userId, String keyName, String apiKey, Date created, Date expires) {
        this(userId,keyName);
        this.apiKey = apiKey;
        this.created = created;
        this.expires = expires;
    }

    public String getUserId() {
        return userId;
    }

    public String getKeyName() {
        return keyName;
    }

    public String getApiKey() {
        return apiKey;
    }

    public Date getCreated() {
        return created;
    }

    public Date getExpires() {
        return expires;
    }
}
