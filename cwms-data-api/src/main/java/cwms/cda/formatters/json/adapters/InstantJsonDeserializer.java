package cwms.cda.formatters.json.adapters;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import cwms.cda.helpers.DateUtils;
import java.io.IOException;
import java.time.Instant;

public class InstantJsonDeserializer extends StdDeserializer<Instant> {

    public InstantJsonDeserializer() {
        this(null);
    }

    protected InstantJsonDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        return DateUtils.parseUserDate(p.getText(), "UTC").toInstant();
    }
}
