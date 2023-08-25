package cwms.cda.formatters.json.adapters;

import java.io.IOException;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import cwms.cda.helpers.DateUtils;

public class ZonedDateTimeJsonDeserializer extends StdDeserializer<ZonedDateTime> {

    public ZonedDateTimeJsonDeserializer() {
        this(null);
    }

    protected ZonedDateTimeJsonDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        String text = p.getText();
        return DateUtils.parseUserDate(text, "UTC");
    }
    
}
