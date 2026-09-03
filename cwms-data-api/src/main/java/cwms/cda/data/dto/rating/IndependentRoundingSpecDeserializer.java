package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser;

import java.io.IOException;

public class IndependentRoundingSpecDeserializer extends JsonDeserializer<IndependentRoundingSpec> {
    @Override
    public IndependentRoundingSpec deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        Integer position = null;
        String value = null;

        if (p instanceof FromXmlParser) {
            FromXmlParser xmlP = (FromXmlParser) p;
            if (xmlP.getCurrentToken() == JsonToken.START_OBJECT || xmlP.getCurrentToken() == JsonToken.FIELD_NAME) {
                try {
                    String posAttr = xmlP.getStaxReader().getAttributeValue(null, "position");
                    if (posAttr != null) {
                        position = Integer.parseInt(posAttr);
                    }
                } catch (IllegalStateException e) {
                    // Not at START_ELEMENT, ignore
                }
            }
        }

        if (p.getCurrentToken() == JsonToken.START_OBJECT) {
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = p.getCurrentName();
                p.nextToken();
                if ("position".equals(fieldName)) {
                    String posStr = p.getValueAsString();
                    if (posStr != null) {
                        position = Integer.parseInt(posStr);
                    }
                } else if ("value".equals(fieldName) || "".equals(fieldName)) {
                    value = p.getText();
                }
            }
        } else if (p.getCurrentToken() == JsonToken.VALUE_STRING) {
            value = p.getText();
        }

        return new IndependentRoundingSpec(position, value);
    }
}
