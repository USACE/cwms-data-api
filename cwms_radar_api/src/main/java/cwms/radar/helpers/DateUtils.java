package cwms.radar.helpers;

import java.time.Period;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DateUtils {
    private static final Pattern WITH_TZ_INFO = Pattern.compile(".*([-+][0-9]{2}(:[0-9]{2})?|Z|\\[[a-zA-Z]*(\\/[a-zA-Z]*)?\\])$");

    public static ZonedDateTime parseUserDate(String date,String timezone){
        ZoneId tz = ZoneId.of(timezone);
        return parseUserDate(date, tz, ZonedDateTime.now(tz));
    }

    public static ZonedDateTime parseUserDate(String date, ZoneId tz, ZonedDateTime now){

        if( date.startsWith("PT")){
            return parseUserDuration(date,tz,now);
        } else if( date.startsWith("P")){
            return parserUserPeriod(date,tz,now);
        } else {
            return parseFullDate(date,tz);
        }
    }

    private static ZonedDateTime parseFullDate(String date, ZoneId tz) {
        Matcher tzInfo = WITH_TZ_INFO.matcher(date);
        if( tzInfo.matches() ) {
            return ZonedDateTime.parse(date).withZoneSameLocal(tz);
        } else {
            LocalDateTime ldt = LocalDateTime.parse(date);
            ZonedDateTime zdt = ZonedDateTime.of(ldt, tz);
            return zdt;
        }


    }

    private static ZonedDateTime parserUserPeriod(String date, ZoneId tz, ZonedDateTime now) {
        Period period = Period.parse(date);
        ZonedDateTime zdt = now
                  .plusYears(period.getYears())
                  .plusMonths(period.getMonths())
                  .plusDays(period.getDays());
        return zdt;
    }

    private static ZonedDateTime parseUserDuration(String date, ZoneId tz, ZonedDateTime now) {
        Duration duration = Duration.parse(date);
        ZonedDateTime zdt = now.plus(duration);
        return zdt;
    }
}
