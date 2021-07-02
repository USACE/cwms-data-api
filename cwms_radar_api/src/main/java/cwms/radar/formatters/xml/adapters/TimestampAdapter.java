package cwms.radar.formatters.xml.adapters;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class TimestampAdapter extends XmlAdapter<String, Timestamp> {
    @Override
    public Timestamp unmarshal(String v) throws Exception {
        return Timestamp.from(Instant.ofEpochMilli(Long.parseLong(v)));
    }

    @Override
    public String marshal(Timestamp v) throws Exception {
        return String.format("%d", v.getTime());
    }
}
