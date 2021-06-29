package cwms.radar.formatters.xml.adapters;

import java.time.Duration;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class DurationAdapter extends XmlAdapter<String, Duration> {
    @Override
    public Duration unmarshal(String v) throws Exception {
        return Duration.parse(v);
    }

    @Override
    public String marshal(Duration v) throws Exception {
        return v.toString();
    }
}
