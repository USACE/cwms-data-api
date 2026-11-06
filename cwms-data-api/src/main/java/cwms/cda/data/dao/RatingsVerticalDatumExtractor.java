package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import cwms.cda.data.dto.VerticalDatumInfo;

import java.util.List;
import java.util.Optional;

public class RatingsVerticalDatumExtractor {

    private RatingsVerticalDatumExtractor() {
        throw new AssertionError("Utility class, don't instantiate");
    }

    public static Optional<VerticalDatum> getVerticalDatum(String ratingSet) {
        return Optional.ofNullable(ratingSet)
                .flatMap(RatingsVerticalDatumExtractor::getVerticalDatumInfo)
                .map(VerticalDatumInfo::getNativeDatum)
                .filter(s -> !s.isEmpty())
                .map(VerticalDatum::getVerticalDatum);
    }

    public static Optional<VerticalDatumInfo> getVerticalDatumInfo(String ratingSet) {
        try {
            return extractVerticalDatumInfo(ratingSet).map(RatingsVerticalDatumExtractor::deserializeVerticalDatumInfoXml);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse Vertical Datum Info", e);
        }
    }

    public static VerticalDatumInfo deserializeVerticalDatumInfoXml(String vdiXml) {
        XmlMapper xmlMapper = new XmlMapper();
        try {
            return xmlMapper.readValue(vdiXml, VerticalDatumInfo.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse Vertical Datum Info", e);
        }
    }

    private static Optional<String> extractVerticalDatumInfo(String ratingSet) throws JsonProcessingException {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode node = xmlMapper.readTree(ratingSet);
        List<JsonNode> values = node.findValues("vertical-datum-info");
        Optional<String> retVal = Optional.empty();
        if (!values.isEmpty()) {
            JsonNode vdiNode = values.get(values.size() - 1);
            retVal = Optional.ofNullable(xmlMapper.writer()
                    .withRootName("vertical-datum-info")
                    .writeValueAsString(vdiNode));
        }
        return retVal;
    }
}
