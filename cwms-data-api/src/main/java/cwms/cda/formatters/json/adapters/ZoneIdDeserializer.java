package cwms.cda.formatters.json.adapters;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import cwms.cda.helpers.ZoneIdHelper;
import java.io.IOException;
import java.time.ZoneId;

public class ZoneIdDeserializer extends JsonDeserializer<ZoneId> {

    @Override
    public ZoneId deserialize(JsonParser p, DeserializationContext deserializationContext) throws IOException {
        return ZoneIdHelper.parseZoneIdWithAliases(p.getValueAsString());
    }
}
