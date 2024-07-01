package cwms.cda.formatters.xml.adapters;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class TimestampAdapter extends XmlAdapter<Long, Timestamp> {
    @Override
    public Timestamp unmarshal(Long v) throws Exception {
        return Timestamp.from(Instant.ofEpochMilli(v));
    }

    @Override
    public Long marshal(Timestamp v) throws Exception {
        return v.getTime();
    }
}
