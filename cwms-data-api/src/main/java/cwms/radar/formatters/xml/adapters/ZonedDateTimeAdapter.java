package cwms.radar.formatters.xml.adapters;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class ZonedDateTimeAdapter extends XmlAdapter<String, ZonedDateTime> {
    DateTimeFormatter formatter = DateTimeFormatter.ISO_ZONED_DATE_TIME;

    @Override
    public ZonedDateTime unmarshal(String v) throws Exception {
        return ZonedDateTime.from(formatter.parse(v));
    }

    @Override
    public String marshal(ZonedDateTime v) throws Exception {
        return formatter.format(v);
    }

}
