package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.type.LogicalType;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import cwms.cda.data.dto.rating.document.RatingsDocument;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;


public final class JsonRatingUtils {

    private static final ObjectMapper JSON_MAPPER = JsonMapper.builder()
            // Repeated xml elements that only occur once are written (and may be sent back)
            // as a plain object instead of a one element array.
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
            .enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED)
            // An empty element such as <rating-points/> is "" in json, read it back as an empty object.
            .withCoercionConfig(LogicalType.POJO,
                    cfg -> cfg.setCoercion(CoercionInputShape.EmptyString, CoercionAction.AsEmpty))
            .build();

    private static final XmlMapper XML_MAPPER = XmlMapper.builder()
            .defaultUseWrapper(false)
            // Lets an aliased or stray single element land in a List property.
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
            .build();

    private JsonRatingUtils() {
    }

    public static RatingSet fromJson(String json) throws RatingException {
        try {
            return toRatingSet(readJson(json));
        } catch (JsonProcessingException e) {
            throw new RatingException(e);
        }
    }

    public static String toJson(RatingSet ratingSet) throws RatingException {
        String retval = null;
        if (ratingSet != null) {
            try {
                retval = writeJson(fromRatingSet(ratingSet));
            } catch (JsonProcessingException e) {
                throw new RatingException(e);
            }
        }
        return retval;
    }

    public static RatingsDocument fromRatingSet(RatingSet ratingSet)
            throws JsonProcessingException {
        return readXml(RatingXmlFactory.toXml(ratingSet, " "));
    }

    public static RatingSet toRatingSet(RatingsDocument ratings)
            throws RatingException, JsonProcessingException {
        return RatingXmlFactory.ratingSet(writeXml(ratings));
    }

    public static RatingsDocument readJson(String json) throws JsonProcessingException {
        return JSON_MAPPER.readValue(json, RatingsDocument.class);
    }

    public static String writeJson(RatingsDocument ratings) throws JsonProcessingException {
        JsonNode tree = JSON_MAPPER.valueToTree(ratings);
        replaceEmptyObjectsWithEmptyStrings(tree);
        return JSON_MAPPER.writeValueAsString(tree);
    }

    // The legacy json was a direct conversion of the xml tree, where an empty element such as
    // <rating-points/> became "" rather than {}.
    private static void replaceEmptyObjectsWithEmptyStrings(JsonNode node) {
        if (node instanceof ObjectNode) {
            ObjectNode object = (ObjectNode) node;
            List<String> emptyFields = new ArrayList<>();
            for (Map.Entry<String, JsonNode> field : object.properties()) {
                if (isEmptyObject(field.getValue())) {
                    emptyFields.add(field.getKey());
                } else {
                    replaceEmptyObjectsWithEmptyStrings(field.getValue());
                }
            }
            emptyFields.forEach(name -> object.put(name, ""));
        } else if (node instanceof ArrayNode) {
            ArrayNode array = (ArrayNode) node;
            for (int i = 0; i < array.size(); i++) {
                if (isEmptyObject(array.get(i))) {
                    array.set(i, TextNode.valueOf(""));
                } else {
                    replaceEmptyObjectsWithEmptyStrings(array.get(i));
                }
            }
        }
    }

    private static boolean isEmptyObject(JsonNode node) {
        return node.isObject() && node.isEmpty();
    }

    public static RatingsDocument readXml(String xml) throws JsonProcessingException {
        return XML_MAPPER.readValue(xml, RatingsDocument.class);
    }

    public static String writeXml(RatingsDocument ratings) throws JsonProcessingException {
        RatingsDocument dbRatings = new RatingsDocument.Builder(ratings)
                .withNoNamespaceSchemaLocation(null)
                .build();
        return XML_MAPPER.writeValueAsString(dbRatings);
    }

    public static String jsonToXml(String json) throws JsonProcessingException {
        return writeXml(readJson(json));
    }

    public static String xmlToJson(String xml) throws JsonProcessingException {
        return writeJson(readXml(xml));
    }
}
