/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.rating.document.CaseElement;
import cwms.cda.data.dto.rating.document.HeightOffsetsElement;
import cwms.cda.data.dto.rating.document.HeightShiftsElement;
import cwms.cda.data.dto.rating.document.IndParameterSpecElement;
import cwms.cda.data.dto.rating.document.IndParameterSpecsElement;
import cwms.cda.data.dto.rating.document.IndRoundingSpecsElement;
import cwms.cda.data.dto.rating.document.OtherIndElement;
import cwms.cda.data.dto.rating.document.PointElement;
import cwms.cda.data.dto.rating.document.PositionalValueElement;
import cwms.cda.data.dto.rating.document.RatingPointsElement;
import cwms.cda.data.dto.rating.document.RatingSpecElement;
import cwms.cda.data.dto.rating.document.RatingTemplateElement;
import cwms.cda.data.dto.rating.document.RatingsDocument;
import cwms.cda.data.dto.rating.document.SelectElement;
import cwms.cda.data.dto.rating.document.SimpleRatingElement;
import cwms.cda.data.dto.rating.document.SourceRatingElement;
import cwms.cda.data.dto.rating.document.TransitionalRatingElement;
import cwms.cda.data.dto.rating.document.TransitionalSourceRatingsElement;
import cwms.cda.data.dto.rating.document.UsgsStreamRatingElement;
import cwms.cda.data.dto.rating.document.VirtualRatingElement;
import cwms.cda.data.dto.rating.document.VirtualSourceRatingsElement;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Converts the json returned by GET /ratings (CWMS_RATING.RETRIEVE_RATINGS_F) into a
 * RatingsDocument so that it can be sent back to POST /ratings.
 */
final class RetrievedRatingsJsonConverter {

    static final String ROOT = "ratings";

    private static final String RATING_TEMPLATES = "rating-templates";
    private static final String RATING_SPECS = "rating-specs";
    private static final String RATINGS = "ratings";
    private static final String SIMPLE_RATING = "simple-rating";
    private static final String USGS_STREAM_RATING = "usgs-stream-rating";
    private static final String VIRTUAL_RATING = "virtual-rating";
    private static final String TRANSITIONAL_RATING = "transitional-rating";

    private static final String OFFICE = "office";
    private static final String NAME = "name";
    private static final String VERSION = "version";
    private static final String DESCRIPTION = "description";
    private static final String EFFECTIVE_DATE = "effective-date";
    private static final String IND_PARAMETERS = "ind-parameters";
    private static final String DEP_PARAMETER = "dep-parameter";
    private static final String VALUES = "values";
    private static final String EXTENSION_VALUES = "extension-values";
    private static final String REFERENCES = "references";
    private static final String TRUE = "true";

    private RetrievedRatingsJsonConverter() {
        throw new AssertionError("Utility class");
    }

    static boolean isRetrievedFormat(ObjectMapper mapper, String json) throws JsonProcessingException {
        try (JsonParser parser = mapper.createParser(json)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return false;
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String fieldName = parser.currentName();
                JsonToken value = parser.nextToken();
                if (ROOT.equals(fieldName) && value == JsonToken.START_OBJECT) {
                    return true;
                }
                parser.skipChildren();
            }
            return false;
        } catch (JsonProcessingException e) {
            throw e;
        } catch (IOException e) {
            // Only reading from a String, should not happen.
            throw new UncheckedIOException(e);
        }
    }

    static RatingsDocument toRatingsDocument(ObjectMapper mapper, String json) throws JsonProcessingException {
        JsonNode ratings = mapper.readTree(json).get(ROOT);
        if (ratings.hasNonNull("error")) {
            throw error("The ratings json holds an error instead of ratings: " + ratings.get("error").asText());
        }

        List<RatingTemplateElement> templates = new ArrayList<>();
        for (JsonNode template : elements(ratings.get(RATING_TEMPLATES))) {
            templates.add(toTemplate(template));
        }

        List<RatingSpecElement> specs = new ArrayList<>();
        for (JsonNode spec : elements(ratings.get(RATING_SPECS))) {
            specs.add(toSpec(spec));
        }

        List<SimpleRatingElement> simpleRatings = new ArrayList<>();
        List<UsgsStreamRatingElement> usgsStreamRatings = new ArrayList<>();
        List<VirtualRatingElement> virtualRatings = new ArrayList<>();
        List<TransitionalRatingElement> transitionalRatings = new ArrayList<>();
        for (JsonNode rating : elements(ratings.get(RATINGS))) {
            // Each entry is keyed by its rating type, e.g. {"simple-rating": {...}}
            for (Map.Entry<String, JsonNode> entry : rating.properties()) {
                switch (entry.getKey()) {
                    case SIMPLE_RATING:
                        simpleRatings.add(toSimpleRating(entry.getValue()));
                        break;
                    case USGS_STREAM_RATING:
                        usgsStreamRatings.add(toUsgsStreamRating(entry.getValue()));
                        break;
                    case VIRTUAL_RATING:
                        virtualRatings.add(toVirtualRating(entry.getValue()));
                        break;
                    case TRANSITIONAL_RATING:
                        transitionalRatings.add(toTransitionalRating(entry.getValue()));
                        break;
                    default:
                        throw error("Unknown rating type '" + entry.getKey() + "'.");
                }
            }
        }

        return new RatingsDocument.Builder()
                .withRatingTemplates(nullIfEmpty(templates))
                .withRatingSpecs(nullIfEmpty(specs))
                .withSimpleRatings(nullIfEmpty(simpleRatings))
                .withUsgsStreamRatings(nullIfEmpty(usgsStreamRatings))
                .withVirtualRatings(nullIfEmpty(virtualRatings))
                .withTransitionalRatings(nullIfEmpty(transitionalRatings))
                .build();
    }

    private static RatingTemplateElement toTemplate(JsonNode template) {
        List<IndParameterSpecElement> indParameterSpecs = new ArrayList<>();
        int position = 1;
        for (JsonNode indParameter : elements(template.get(IND_PARAMETERS))) {
            indParameterSpecs.add(new IndParameterSpecElement.Builder()
                    .withPosition(position++)
                    .withParameter(text(indParameter, NAME))
                    .withInRangeMethod(text(indParameter, "value-lookup-in-range"))
                    .withOutRangeLowMethod(text(indParameter, "value-lookup-below-range"))
                    .withOutRangeHighMethod(text(indParameter, "value-lookup-above-range"))
                    .build());
        }

        String version = text(template, VERSION);
        String parametersId = text(template, "parameters-string");
        if (parametersId == null) {
            parametersId = removeSuffix(text(template, NAME), version);
        }

        return new RatingTemplateElement.Builder()
                .withOfficeId(text(template, OFFICE))
                .withParametersId(parametersId)
                .withVersion(version)
                .withIndParameterSpecs(new IndParameterSpecsElement.Builder()
                        .withIndParameterSpecs(indParameterSpecs)
                        .build())
                .withDepParameter(text(template, DEP_PARAMETER))
                .withDescription(textOrDefault(template, DESCRIPTION, ""))
                .build();
    }

    private static RatingSpecElement toSpec(JsonNode spec) {
        String ratingSpecId = text(spec, NAME);
        String locationId = text(spec, "location");
        String version = text(spec, VERSION);

        JsonNode rounding = spec.path("rounding");
        List<PositionalValueElement> indRoundingSpecs = new ArrayList<>();
        int position = 1;
        for (JsonNode indRounding : elements(rounding.get(IND_PARAMETERS))) {
            indRoundingSpecs.add(new PositionalValueElement.Builder()
                    .withPosition(position++)
                    .withValue(indRounding.asText())
                    .build());
        }

        // The retrieved json does not include the active or auto-* flags, so default to an
        // active spec with the auto-* behavior turned off when they're missing.
        return new RatingSpecElement.Builder()
                .withOfficeId(text(spec, OFFICE))
                .withRatingSpecId(ratingSpecId)
                .withTemplateId(templateId(ratingSpecId, locationId, version))
                .withLocationId(locationId)
                .withVersion(version)
                .withSourceAgency(textOrDefault(spec, "source-agency", ""))
                .withInRangeMethod(text(spec, "time-lookup-in-range"))
                .withOutRangeLowMethod(text(spec, "time-lookup-before-first"))
                .withOutRangeHighMethod(text(spec, "time-lookup-after-last"))
                .withActive(textOrDefault(spec, "active", TRUE))
                .withAutoUpdate(textOrDefault(spec, "auto-update", "false"))
                .withAutoActivate(textOrDefault(spec, "auto-activate", "false"))
                .withAutoMigrateExtension(textOrDefault(spec, "auto-migrate-extension", "false"))
                .withIndRoundingSpecs(indRoundingSpecs.isEmpty() ? null
                        : new IndRoundingSpecsElement.Builder()
                                .withIndRoundingSpecs(indRoundingSpecs)
                                .build())
                .withDepRoundingSpec(text(rounding, DEP_PARAMETER))
                .withDescription(textOrDefault(spec, DESCRIPTION, ""))
                .build();
    }

    private static SimpleRatingElement toSimpleRating(JsonNode rating) throws JsonMappingException {
        String ratingSpecId = text(rating, NAME);
        // e.g. ["Stage (ft)"] and "Flow (cfs)"
        List<String> indParameters = texts(rating.get(IND_PARAMETERS));

        SimpleRatingElement.Builder builder = new SimpleRatingElement.Builder()
                .withOfficeId(text(rating, OFFICE))
                .withRatingSpecId(ratingSpecId)
                .withUnitsId(unitsId(indParameters, text(rating, DEP_PARAMETER)))
                .withEffectiveDate(text(rating, EFFECTIVE_DATE))
                .withActive(TRUE)
                .withDescription(textOrDefault(rating, DESCRIPTION, ""))
                .withFormula(text(rating, "formula"));

        JsonNode values = rating.get(VALUES);
        if (values != null && !values.isNull()) {
            builder.withRatingPoints(nullIfEmpty(
                    toRatingPoints(ratingSpecId, VALUES, values, Math.max(indParameters.size(), 1))));
        }
        builder.withExtensionPoints(extensionPoints(ratingSpecId, rating));
        return builder.build();
    }

    private static UsgsStreamRatingElement toUsgsStreamRating(JsonNode rating) throws JsonMappingException {
        String ratingSpecId = text(rating, NAME);
        String indParameter = text(rating, "ind-parameter");
        // The xsl also puts the datum on the dep parameter label when the ind parameter is an elevation.
        String unitsId = indParameter == null ? null : unitsId(Collections.singletonList(indParameter),
                text(rating, DEP_PARAMETER), isElevation(indParameter));

        List<HeightShiftsElement> shifts = new ArrayList<>();
        for (JsonNode shift : elements(rating.get("shifts"))) {
            shifts.add(new HeightShiftsElement.Builder()
                    .withEffectiveDate(text(shift, EFFECTIVE_DATE))
                    .withActive(TRUE)
                    .withPoints(nullIfEmpty(toPoints(ratingSpecId, "shifts", shift.get(VALUES))))
                    .build());
        }

        // The database always writes <height-offsets/> and <rating-points/> for these, even when
        // empty (e.g. a rating that only holds shifts for an existing base curve).
        return new UsgsStreamRatingElement.Builder()
                .withOfficeId(text(rating, OFFICE))
                .withRatingSpecId(ratingSpecId)
                .withUnitsId(unitsId)
                .withEffectiveDate(text(rating, EFFECTIVE_DATE))
                .withActive(TRUE)
                .withDescription(textOrDefault(rating, DESCRIPTION, ""))
                .withHeightShifts(nullIfEmpty(shifts))
                .withHeightOffsets(new HeightOffsetsElement.Builder()
                        .withPoints(nullIfEmpty(toPoints(ratingSpecId, "offsets", rating.get("offsets"))))
                        .build())
                .withRatingPoints(Collections.singletonList(new RatingPointsElement.Builder()
                        .withPoints(nullIfEmpty(toPoints(ratingSpecId, VALUES, rating.get(VALUES))))
                        .build()))
                .withExtensionPoints(extensionPoints(ratingSpecId, rating))
                .build();
    }

    private static VirtualRatingElement toVirtualRating(JsonNode rating) {
        // The xsl splits the connections on ',' into an array.
        String connections = String.join(",", texts(rating.get("connections")));

        // References are written as [{"name": "R1", "reference": "...", "units": "ft;ft"}]
        List<SourceRatingElement> sourceRatings = new ArrayList<>();
        int index = 1;
        for (JsonNode reference : elements(rating.get(REFERENCES))) {
            String value = text(reference, "reference");
            String units = text(reference, "units");
            String valueWithUnits = value != null && units != null ? value + " {" + units + "}" : value;
            SourceRatingElement.Builder sourceRating = new SourceRatingElement.Builder()
                    .withPosition(position(text(reference, NAME), index++));
            if (isRatingSpecId(value)) {
                sourceRating.withRatingSpecId(valueWithUnits);
            } else {
                sourceRating.withRatingExpression(valueWithUnits);
            }
            sourceRatings.add(sourceRating.build());
        }

        return new VirtualRatingElement.Builder()
                .withOfficeId(text(rating, OFFICE))
                .withRatingSpecId(text(rating, NAME))
                .withEffectiveDate(text(rating, EFFECTIVE_DATE))
                .withActive(TRUE)
                .withDescription(textOrDefault(rating, DESCRIPTION, ""))
                .withConnections(connections.isEmpty() ? null : connections)
                .withSourceRatings(new VirtualSourceRatingsElement.Builder()
                        .withSourceRatings(nullIfEmpty(sourceRatings))
                        .build())
                .build();
    }

    private static TransitionalRatingElement toTransitionalRating(JsonNode rating) {
        List<CaseElement> cases = new ArrayList<>();
        int position = 1;
        for (JsonNode caseNode : elements(rating.get("cases"))) {
            cases.add(new CaseElement.Builder()
                    .withPosition(position++)
                    .withWhen(text(caseNode, "when"))
                    .withThen(text(caseNode, "then"))
                    .build());
        }

        // References are written as [{"R1": "rating-spec-id"}, {"R2": "rating-spec-id"}]
        List<PositionalValueElement> sourceRatings = new ArrayList<>();
        int index = 1;
        for (JsonNode reference : elements(rating.get(REFERENCES))) {
            for (Map.Entry<String, JsonNode> entry : reference.properties()) {
                sourceRatings.add(new PositionalValueElement.Builder()
                        .withPosition(position(entry.getKey(), index++))
                        .withValue(entry.getValue().isNull() ? null : entry.getValue().asText())
                        .build());
            }
        }

        return new TransitionalRatingElement.Builder()
                .withOfficeId(text(rating, OFFICE))
                .withRatingSpecId(text(rating, NAME))
                .withUnitsId(unitsId(texts(rating.get(IND_PARAMETERS)), text(rating, DEP_PARAMETER)))
                .withEffectiveDate(text(rating, EFFECTIVE_DATE))
                .withActive(TRUE)
                .withDescription(textOrDefault(rating, DESCRIPTION, ""))
                .withSelect(new SelectElement.Builder()
                        .withCases(nullIfEmpty(cases))
                        .withDefault(text(rating, "default"))
                        .build())
                .withSourceRatings(new TransitionalSourceRatingsElement.Builder()
                        .withRatingSpecIds(nullIfEmpty(sourceRatings))
                        .build())
                .build();
    }

    private static List<RatingPointsElement> extensionPoints(String ratingSpecId, JsonNode rating)
            throws JsonMappingException {
        return nullIfEmpty(toRatingPoints(ratingSpecId, EXTENSION_VALUES, rating.get(EXTENSION_VALUES), 1));
    }

    private static List<PointElement> toPoints(String ratingSpecId, String field, JsonNode rows)
            throws JsonMappingException {
        List<RatingPointsElement> ratingPoints = toRatingPoints(ratingSpecId, field, rows, 1);
        return ratingPoints.isEmpty() ? Collections.emptyList() : ratingPoints.get(0).getPoints();
    }

    /**
     * Each row is [ind-1, ..., ind-n, dep] with an optional trailing note. Rows sharing the same
     * leading independent values are grouped into one rating-points element with those values as
     * other-ind entries, matching the layout of the rating xml.
     */
    private static List<RatingPointsElement> toRatingPoints(String ratingSpecId, String field, JsonNode rows,
                                                            int indCount) throws JsonMappingException {
        List<RatingPointsElement> retval = new ArrayList<>();
        if (rows == null || rows.isNull()) {
            return retval;
        }
        if (!rows.isArray()) {
            throw error("Rating " + ratingSpecId + ": expected '" + field + "' to be an array of rows.");
        }
        List<String> currentOtherInds = null;
        List<PointElement> points = new ArrayList<>();
        for (JsonNode row : rows) {
            int size = row.isArray() ? row.size() : -1;
            String note = null;
            if (size == indCount + 2 && row.get(size - 1).isTextual()) {
                note = row.get(size - 1).asText();
                size--;
            }
            if (size != indCount + 1) {
                throw badRow(ratingSpecId, field, row, indCount);
            }
            List<String> otherInds = new ArrayList<>();
            for (int i = 0; i < indCount - 1; i++) {
                otherInds.add(row.get(i).asText());
            }
            if (currentOtherInds != null && !currentOtherInds.equals(otherInds)) {
                retval.add(ratingPoints(currentOtherInds, points));
                points = new ArrayList<>();
            }
            currentOtherInds = otherInds;
            points.add(new PointElement.Builder()
                    .withInd(row.get(indCount - 1).asText())
                    .withDep(row.get(indCount).asText())
                    .withNote(note)
                    .build());
        }
        if (currentOtherInds != null) {
            retval.add(ratingPoints(currentOtherInds, points));
        }
        return retval;
    }

    private static JsonMappingException badRow(String ratingSpecId, String field, JsonNode row, int indCount) {
        boolean onlyLastInd = row.isArray() && (row.size() == 2 || (row.size() == 3 && row.get(2).isTextual()));
        if (indCount > 1 && onlyLastInd) {
            // Ratings_v1_to_RADAR_json.xsl drops the other-ind values, so the table can't be rebuilt.
            return error("Rating " + ratingSpecId + " has " + indCount + " independent parameters but the "
                    + "GET /ratings json only holds the last independent and the dependent value of each point. "
                    + "Use the json or xml from GET /ratings/{rating-id} to store this rating.");
        }
        return error("Rating " + ratingSpecId + ": expected each row of '" + field + "' to hold " + (indCount + 1)
                + " numbers (" + indCount + " independent followed by the dependent) and an optional note, "
                + "but found: " + row);
    }

    private static RatingPointsElement ratingPoints(List<String> otherIndValues, List<PointElement> points) {
        List<OtherIndElement> otherInds = new ArrayList<>();
        for (int i = 0; i < otherIndValues.size(); i++) {
            otherInds.add(new OtherIndElement.Builder()
                    .withPosition(i + 1)
                    .withValue(otherIndValues.get(i))
                    .build());
        }
        return new RatingPointsElement.Builder()
                .withOtherInds(nullIfEmpty(otherInds))
                .withPoints(points)
                .build();
    }

    static String unitsId(List<String> indParameters, String depParameter) {
        return unitsId(indParameters, depParameter, isElevation(depParameter));
    }

    private static String unitsId(List<String> indParameters, String depParameter, boolean depHasDatum) {
        String depUnit = unit(depParameter, depHasDatum);
        if (depUnit == null || indParameters.isEmpty()) {
            return null;
        }
        List<String> indUnits = new ArrayList<>();
        for (String indParameter : indParameters) {
            String indUnit = unit(indParameter, isElevation(indParameter));
            if (indUnit == null) {
                return null;
            }
            indUnits.add(indUnit);
        }
        return String.join(",", indUnits) + ";" + depUnit;
    }

    // Elevation labels may have the vertical datum after the unit, e.g. "Elev (ft NAVD88)".
    private static String unit(String parameter, boolean mayHaveDatum) {
        if (parameter == null) {
            return null;
        }
        int open = parameter.lastIndexOf('(');
        int close = parameter.lastIndexOf(')');
        if (open < 0 || close < open) {
            return null;
        }
        String unit = parameter.substring(open + 1, close).trim();
        int space = unit.indexOf(' ');
        if (mayHaveDatum && space > 0) {
            unit = unit.substring(0, space);
        }
        return unit;
    }

    private static boolean isElevation(String parameter) {
        return parameter != null && parameter.startsWith("Elev");
    }

    // location.template-parameters.template-version.spec-version, where the template parameters
    // hold a ';'. Anything else referenced by a virtual rating is an expression.
    static boolean isRatingSpecId(String reference) {
        if (reference == null) {
            return false;
        }
        String[] parts = reference.split("\\.", -1);
        if (parts.length != 4 || !parts[1].contains(";")) {
            return false;
        }
        for (String part : parts) {
            if (part.trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    // Source ratings are named "R1", "R2"... and the number is the position.
    private static String position(String name, int defaultPosition) {
        if (name != null && name.length() > 1 && Character.toUpperCase(name.charAt(0)) == 'R') {
            return name.substring(1);
        }
        return String.valueOf(defaultPosition);
    }

    static String templateId(String ratingSpecId, String locationId, String version) {
        if (ratingSpecId == null) {
            return null;
        }
        String retval = ratingSpecId;
        if (locationId != null && retval.startsWith(locationId + ".")) {
            retval = retval.substring(locationId.length() + 1);
        } else if (retval.indexOf('.') >= 0) {
            retval = retval.substring(retval.indexOf('.') + 1);
        }
        if (version != null && retval.endsWith("." + version)) {
            retval = removeSuffix(retval, version);
        } else if (retval.lastIndexOf('.') >= 0) {
            retval = retval.substring(0, retval.lastIndexOf('.'));
        }
        return retval;
    }

    private static String removeSuffix(String value, String suffix) {
        if (value == null || suffix == null || !value.endsWith("." + suffix)) {
            return value;
        }
        return value.substring(0, value.length() - suffix.length() - 1);
    }

    private static List<JsonNode> elements(JsonNode node) {
        List<JsonNode> retval = new ArrayList<>();
        if (node == null || node.isNull() || node.isMissingNode()) {
            return retval;
        }
        if (node.isArray()) {
            node.forEach(retval::add);
        } else {
            retval.add(node);
        }
        return retval;
    }

    private static List<String> texts(JsonNode node) {
        List<String> retval = new ArrayList<>();
        for (JsonNode element : elements(node)) {
            if (!element.isNull()) {
                retval.add(element.asText());
            }
        }
        return retval;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        return value.asText();
    }

    private static String textOrDefault(JsonNode node, String field, String defaultValue) {
        String value = text(node, field);
        return value == null ? defaultValue : value;
    }

    private static <T> List<T> nullIfEmpty(List<T> values) {
        return values == null || values.isEmpty() ? null : values;
    }

    private static JsonMappingException error(String message) {
        return new JsonMappingException(null, message);
    }
}
