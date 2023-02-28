package cwms.radar.helpers;

import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import java.util.regex.Pattern;


public class DateUtils {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    // This pattern matches if a string ends with an offset (like -07:00 or -0700),
    // or a Z,
    // or a named timezone (like [UTC] or [US/Pacific])
    // The pattern is not concerned with the date/time part
    private static final Pattern WITH_TZ_INFO = Pattern.compile(".*"
            + "("   // followed by
            + "[-+]\\d{2}"   // 2 digit hour offset
            + "(:?\\d{2})?"  // optional colon and 2 digit minute offset
            + "|Z" // OR Z
            + "|\\[" // OR an open square bracket
            + "[a-zA-Z0-9]*" // any number of letters and numbers
            + "(/[a-zA-Z_]*)?" // optionally followed by a / and more letters or underbar as in Los_Angeles
            + "]" // followed by a close square bracket
            + ")"
            + "$" // then String must immediately end
            );

    private DateUtils() {
        // utility class
    }

    /**
     * Parse a string into a ZonedDateTime.
     * @param text The string to parse
     * @param timezone The timezone to use if the string doesn't have one.
     * @return The parsed ZonedDateTime
     */
    public static ZonedDateTime parseUserDate(String text, String timezone) {
        ZoneId tz = ZoneId.of(timezone);
        return parseUserDate(text, tz, ZonedDateTime.now(tz));
    }

    public static ZonedDateTime parseUserDate(String text, ZoneId tz, ZonedDateTime now) {

        if (text.startsWith("PT")) {
            return parseUserDuration(text, now);
        } else if (text.startsWith("P")) {
            return parserUserPeriod(text, now);
        } else {
            return parseFullDate(text, tz);
        }
    }

    private static ZonedDateTime parseFullDate(String text, ZoneId tz) {

        if (hasZone(text)) {
            return parseZonedDateTime(text);
        } else {
            // no timezone info "2021-04-05T00:00:00" (notice there is no Z) assume local time
            // If it had an offset it would have gone into the other branch
            TemporalAccessor dt = DateTimeFormatter.ISO_LOCAL_DATE_TIME
                    .parseBest(text.trim(), LocalDateTime::from, Instant::from, LocalDate::from);
            if (dt instanceof LocalDateTime) {
                LocalDateTime ldt = (LocalDateTime) dt;
                return ZonedDateTime.of(ldt, tz);
            } else if (dt instanceof Instant) {
                Instant instant = (Instant) dt;
                return instant.atZone(tz);
            } else if (dt instanceof LocalDate) {
                LocalDate ld = (LocalDate) dt;
                return ld.atStartOfDay(tz);
            } else {
                throw new DateTimeParseException("Unable to parse date", text, 0);
            }

        }
    }

    /**
     * Determines if a string ends with any of three common ways to denote timezone information.
     *  1. An offset like -07:00 or -0700   Matches "2021-04-05T00:00:00-07:00"
     *  2. Z   Matches "2021-04-05T00:00:00Z"
     *  3. A named timezone like [UTC] or [US/Pacific]  Matches "2021-04-05T00:00:00[UTC]"
     *
     * @param text The string to check
     * @return true if the string ends with a timezone indicator
     */
    public static boolean hasZone(String text) {
        return WITH_TZ_INFO.matcher(text).matches();
    }


    private static ZonedDateTime parseZonedDateTime(String text) {
        ZonedDateTime zdt;
        try {
            // Try to use the default ZonedDateTime format first.
            zdt = ZonedDateTime.parse(text);
        } catch (DateTimeParseException e) {
            //  https://stackoverflow.com/questions/66812557/how-to-parse-a-date-with-timezone-with-and-without-colon/66812678#66812678
            // To match 2022-01-19T20:52:53+0000[UTC]
            String[] possibleDateFormats = { "yyyy-MM-dd'T'HH:mm:ssZ"};  // Can add formats as needed
            Optional<ZonedDateTime> parsed = firstMatch(text, possibleDateFormats);
            if(parsed.isPresent()) {
                zdt = parsed.get();
            } else {
                throw e;
            }
        }

        return zdt;
    }

    private static Optional<ZonedDateTime> firstMatch(String text, String[] possibleDateFormats) {
        Optional<ZonedDateTime> retval = Optional.empty();

        if (possibleDateFormats != null) {
            for (String format : possibleDateFormats) {
                retval = parseWithPattern(text, format);
                if(retval.isPresent())
                    break;
                }
            }

        return retval;
    }

    public static Optional<ZonedDateTime> parseWithPattern(String text, String pattern) {
        Optional<ZonedDateTime> retval = Optional.empty();
        try {
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
            builder = builder.appendPattern(pattern);
            builder = appendZoneId(builder);
            DateTimeFormatter formatter = builder.toFormatter();

            TemporalAccessor ta = formatter.parse(text);
            retval = Optional.of(ZonedDateTime.from(ta));
        } catch (DateTimeParseException e2) {
            // mostly ignore
            logger.atFinest().withCause(e2).log("Unable to parse %s with format %s", text, pattern);
        }
        return retval;
    }

    private static DateTimeFormatterBuilder appendZoneId(DateTimeFormatterBuilder builder) {
        return builder.optionalStart()  // This is to allow for bracket zoneId like [Europe/Paris]
            .appendLiteral('[')
            .parseCaseSensitive()
            .appendZoneRegionId()
            .appendLiteral(']');
    }

    private static ZonedDateTime parserUserPeriod(String text, ZonedDateTime now) {
        Period period = Period.parse(text);
        return now.plusYears(period.getYears())
                .plusMonths(period.getMonths())
                .plusDays(period.getDays());
    }

    private static ZonedDateTime parseUserDuration(String text, ZonedDateTime now) {
        Duration duration = Duration.parse(text);
        return now.plus(duration);
    }
}
