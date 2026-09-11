package cwms.cda.data.dto.auth;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.TimeSeries;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.ZonedDateTime;

/** Public key information returned after creation, without the secret. */
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class ApiKeyMetadata {
    private final String userId;
    private final String keyName;
    private final ZonedDateTime created;
    private final ZonedDateTime expires;

    /**
     * Copy key information without retaining the secret.
     * @param key key returned by the DAO
     */
    public ApiKeyMetadata(ApiKey key) {
        userId = key.getUserId();
        keyName = key.getKeyName();
        created = key.getCreated();
        expires = key.getExpires();
    }

    @Schema(required = true)
    public String getUserId() {
        return userId;
    }

    @Schema(required = true)
    public String getKeyName() {
        return keyName;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = TimeSeries.ZONED_DATE_TIME_FORMAT)
    @Schema(description = "The instant this key was created, in ISO-8601 format with offset and timezone.")
    public ZonedDateTime getCreated() {
        return created;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = TimeSeries.ZONED_DATE_TIME_FORMAT)
    @Schema(description = "When this key expires, in ISO-8601 format with offset and timezone.")
    public ZonedDateTime getExpires() {
        return expires;
    }
}
