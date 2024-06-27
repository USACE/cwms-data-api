package cwms.cda.data.dto.auth;

import java.time.ZonedDateTime;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.json.adapters.ZonedDateTimeJsonDeserializer;
import cwms.cda.formatters.xml.adapters.ZonedDateTimeAdapter;
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
        description = "The instant this Key was created, in ISO-8601 format with offset and timezone ('" + TimeSeries.ZONED_DATE_TIME_FORMAT + "')"
    )
    private ZonedDateTime created;

    @JsonDeserialize(using = ZonedDateTimeJsonDeserializer.class)
    @JsonFormat(shape = Shape.STRING, pattern = TimeSeries.ZONED_DATE_TIME_FORMAT)
    @Schema(
        description = "When this key expires, in ISO-8601 format with offset and timezone ('" + TimeSeries.ZONED_DATE_TIME_FORMAT + "')"
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ApiKey{")
          .append("userId=").append(userId).append(", ")
          .append("keyName=").append(keyName).append(", ")
          .append("apiKey=").append(apiKey == null ? "0" : apiKey.length()).append(" hidden characters, ")
          .append("created=").append(created).append(", ")
          .append("expires=").append(expires)
          .append("}");
        return sb.toString();
    }
}
