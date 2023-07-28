package cwms.cda.data.dto.auth;

import java.time.ZonedDateTime;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import cwms.cda.data.dto.TimeSeries;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.Schema.AccessMode;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class ApiKey {
    @JsonProperty(required = true)
    private String userId;
    @JsonProperty(required = true)
    private String keyName;
    
    private String apiKey;
    @JsonFormat(shape = Shape.STRING, pattern = TimeSeries.ZONED_DATE_TIME_FORMAT)
    @Schema(
        accessMode = AccessMode.READ_ONLY,
        description = "The requested start time of the data, in ISO-8601 format with offset and timezone ('" + TimeSeries.ZONED_DATE_TIME_FORMAT + "')"
    )
    private ZonedDateTime created;
    @JsonFormat(shape = Shape.STRING, pattern = TimeSeries.ZONED_DATE_TIME_FORMAT)
    @Schema(
        description = "The requested start time of the data, in ISO-8601 format with offset and timezone ('" + TimeSeries.ZONED_DATE_TIME_FORMAT + "')"
    )
    private ZonedDateTime expires;

    public ApiKey() {}

    public ApiKey(String userId, String keyName) {
        this.userId = userId;
        this.keyName = keyName;
    }

    public ApiKey(String userId, String keyName, String apiKey, ZonedDateTime created, ZonedDateTime expires) {
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

    public ZonedDateTime getCreated() {
        return created;
    }

    public ZonedDateTime getExpires() {
        return expires;
    }
}
