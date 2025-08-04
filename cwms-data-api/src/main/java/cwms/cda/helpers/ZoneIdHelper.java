package cwms.cda.helpers;

import com.google.common.flogger.FluentLogger;
import org.jetbrains.annotations.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

/**
 * Helper class for handling timezone IDs and their aliases.
 * Provides functionality to map non-standard timezone identifiers to standard ZoneId values.
 */
public class ZoneIdHelper {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    /**
     * Default resource location for timezone aliases properties file.
     * Can be overridden via the system property "cwms.timezone.aliases.resource".
     */
    public static final String RESOURCE_LOCATION = System.getProperty("cwms.timezone.aliases.resource",
            "cwms/cda/data/dao/timezone-aliases.properties");

    private static final Map<String, String> tzAliases = Collections.unmodifiableMap(buildTimeZoneAliases());

    private ZoneIdHelper() {
        // Prevent instantiation
    }

    /**
     * Builds a map of default timezone aliases.
     * These are hardcoded fallback values used when no resource file is available.
     *
     * @return A map of timezone aliases where keys are non-standard timezone IDs and 
     *         values are their corresponding standard timezone IDs
     */
    public static Map<String, String> buildDefaultAliases() {
        Map<String, String> aliases = new HashMap<>();
        aliases.put("Canada/East-Saskatchewan", "Canada/Saskatchewan");
        aliases.put("ROC", "Asia/Taipei");
        aliases.put("US/Pacific-New", "US/Pacific");
        aliases.put("Unknown or Not Applicable", "UTC");
        return aliases;
    }

    /**
     * Builds a map of timezone aliases from the default resource location.
     *
     * @return A map of timezone aliases loaded from the resource file, or an empty map if
     *         the resource cannot be loaded
     */
    public static Map<String, String> buildResourceAliases() {
        return buildResourceAliases(RESOURCE_LOCATION);
    }

    /**
     * Builds a map of timezone aliases from the specified resource location.
     *
     * @param resourceLocation The location of the properties file containing timezone aliases
     * @return A map of timezone aliases loaded from the resource file, or an empty map if
     *         the resource cannot be loaded
     */
    public static @NotNull Map<String, String> buildResourceAliases(@Nullable String resourceLocation) {
        if (resourceLocation == null || resourceLocation.isEmpty()) {
            logger.atWarning().log("Resource location is null or empty");
            return Collections.emptyMap();
        }

        try (InputStream resource = ZoneIdHelper.class.getClassLoader().getResourceAsStream(resourceLocation)) {
            if (resource != null) {
                Properties props = new Properties();
                props.load(resource);
                return buildAliasMap(props);
            } else {
                logger.atWarning().log("Timezone aliases properties file not found at " + resourceLocation);
            }
        } catch (IOException e) {
            logger.atWarning().withCause(e).log("Failed to load timezone aliases from resource file.");
        }
        return Collections.emptyMap();
    }

    /**
     * Builds a complete map of timezone aliases by combining resource-based aliases
     * with default aliases when necessary.
     *
     * @return A map of timezone aliases where keys are non-standard timezone IDs and
     *         values are their corresponding standard timezone IDs
     */
    public static @NotNull Map<String, String> buildTimeZoneAliases() {
        Map<String, String> aliases = buildResourceAliases();

        if (aliases.isEmpty()) {
            logger.atWarning().log("No timezone aliases found in resource file, using default aliases.");
            aliases = buildDefaultAliases();
        }

        return aliases;
    }

    /**
     * Builds a map of timezone aliases from the provided Properties object.
     * The properties should follow the format:
     * alias.N.from=NonStandardTimezoneId
     * alias.N.to=StandardTimezoneId
     * where N is a number.
     *
     * @param props Properties object containing timezone alias definitions
     * @return A map of timezone aliases parsed from the properties
     */
    public static @NotNull Map<String, String> buildAliasMap(@Nullable Properties props) {
        if (props == null) {
            return Collections.emptyMap();
        }

        Map<String, String> aliases = new LinkedHashMap<>();
        // Find all alias entries by looking for .from properties
        for (String key : props.stringPropertyNames()) {
            if (key.matches("alias\\.\\d+\\.from")) {
                String aliasNumber = key.substring(6, key.lastIndexOf('.'));
                String fromKey = "alias." + aliasNumber + ".from";
                String toKey = "alias." + aliasNumber + ".to";

                String fromValue = props.getProperty(fromKey);
                String toValue = props.getProperty(toKey);

                if (fromValue != null && toValue != null) {
                    aliases.put(fromValue, toValue);
                }
            }
        }
        return aliases;
    }

    /**
     * Parses a string to a ZoneId, using the timezone aliases if necessary.
     *
     * @param zoneId The timezone ID string to parse
     * @return The parsed ZoneId
     * @throws DateTimeException if the zone ID has an invalid format or the zone ID is not available
     */
    public static ZoneId parseZoneIdWithAliases(String zoneId) {

        return ZoneId.of(zoneId, tzAliases);
    }
    
    /**
     * Returns an unmodifiable map of all available timezone aliases.
     *
     * @return An unmodifiable map of timezone aliases where keys are non-standard timezone IDs and
     *         values are their corresponding standard timezone IDs
     */
    public static Map<String, String> getTimezoneAliases() {
        return tzAliases;
    }
}
