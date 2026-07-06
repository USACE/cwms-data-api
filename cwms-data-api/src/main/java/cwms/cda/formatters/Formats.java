/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

package cwms.cda.formatters;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.annotations.FormattableWith;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class Formats {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String PLAIN = "text/plain";    // Only used as a constant, not for any
    // data mapping
    public static final String JSON = "application/json";
    public static final String JSONV1 = "application/json;version=1";
    public static final String JSONV2 = "application/json;version=2";
    public static final String XML = "application/xml";
    public static final String XMLV1 = "application/xml;version=1";
    public static final String XMLV2 = "application/xml;version=2";
    public static final String WML2 = "application/vnd.opengis.waterml+xml";
    public static final String TAB = "text/tab-separated-values";
    public static final String CSV = "text/csv";
    public static final String GEOJSON = "application/geo+json";
    public static final String PGJSON = "application/vnd.pg+json";
    public static final String NAMED_PGJSON = "application/vnd.named+pg+json";
    public static final String RSS = "application/rss+xml";
    public static final String DEFAULT = "*/*";

    public static final String JSON_LEGACY = "json";
    public static final String XML_LEGACY = "xml";
    public static final String WML2_LEGACY = "wml2";
    public static final String TAB_LEGACY = "tab";
    public static final String CSV_LEGACY = "csv";
    public static final String GEOJSON_LEGACY = "geojson";
    public static final String PGJSON_LEGACY = "pgjson";
    public static final String NAMED_PGJSON_LEGACY = "named-pgjson";


    private static final List<ContentType> contentTypeList = new ArrayList<>();

    static {
        contentTypeList.addAll(
                Stream.of(DEFAULT, JSON, JSONV1, XML, XMLV1, XMLV2, RSS, WML2, JSONV2,
                        TAB, CSV, GEOJSON, PGJSON, NAMED_PGJSON)
                        .map(ContentType::new)
                        .collect(Collectors.toList()));
    }

    private static final Map<String, String> typeMap = new LinkedHashMap<>();

    static {
        typeMap.put(JSON_LEGACY, Formats.JSON);
        typeMap.put(XML_LEGACY, Formats.XML);
        typeMap.put(WML2_LEGACY, Formats.WML2);
        typeMap.put(TAB_LEGACY, Formats.TAB);
        typeMap.put(CSV_LEGACY, Formats.CSV);
        typeMap.put(GEOJSON_LEGACY, Formats.GEOJSON);
        typeMap.put(PGJSON_LEGACY, Formats.PGJSON);
        typeMap.put(NAMED_PGJSON_LEGACY, Formats.NAMED_PGJSON);
    }


    private final Map<ContentType, Map<Class<? extends CwmsDTOBase>, OutputFormatter>> formatters =
        new LinkedHashMap<>();

    private static final Formats formats = new Formats();

    private Formats() {
    }

    /**
     * Given the provided content type, get the appropriate legacy content-type.
     * @param contentType given ContentType
     * @return a previously configured contenttype value, or the value of {@link JSON_LEGACY}
     */
    public static String getLegacyTypeFromContentType(ContentType contentType) {
        return typeMap.entrySet()
                      .stream()
                      .filter(e -> e.getValue().equals(contentType.getType()))
                      .map(Map.Entry::getKey)
                      .findFirst()
                      .orElse(JSON_LEGACY);
    }

    private String getFormatted(ContentType type, CwmsDTOBase toFormat) throws FormattingException {
        Objects.requireNonNull(toFormat, "Object to be formatted should not be null");
        formatters.keySet().forEach(k -> logger.atFine().log("%s", k.toString()));
        OutputFormatter outputFormatter = getOutputFormatterInternal(type, toFormat.getClass());

        if (outputFormatter != null) {
            return outputFormatter.format(toFormat);
        } else {
            String message = String.format("No Format for this content-type and data-type : (%s, %s)",
                    type.toString(), toFormat.getClass().getName());
            throw new UnsupportedFormatException(message);
        }

    }

    private String getFormatted(ContentType type, List<? extends CwmsDTOBase> dtos, Class<?
            extends CwmsDTOBase> rootType) throws FormattingException {
        for (ContentType key : formatters.keySet()) {
            logger.atFinest().log("%s", key.toString());
        }

        OutputFormatter outputFormatter = getOutputFormatterInternal(type, rootType);

        if (outputFormatter != null) {
            return outputFormatter.format(dtos);
        } else {
            String message = String.format("No Format for this content-type and data type : (%s, %s)",
                    type.toString(), rootType.getName());
            throw new UnsupportedFormatException(message);
        }
    }

    private <T extends CwmsDTOBase> T parseContentFromType(ContentType type, String content, Class<T> rootType)
            throws FormattingException {
        OutputFormatter outputFormatter = getOutputFormatterInternal(type, rootType);
        if (outputFormatter != null) {
            T retval = outputFormatter.parseContent(content, rootType);
            retval.validate();
            return retval;
        } else {
            String message = String.format("No Format for this content-type and data type : (%s, %s)",
                    type.toString(), rootType.getName());
            throw new UnsupportedFormatException(message);
        }
    }

    private <T extends CwmsDTOBase> T parseContentFromType(ContentType type, InputStream content, Class<T> rootType)
            throws FormattingException {
        OutputFormatter outputFormatter = getOutputFormatterInternal(type, rootType);
        if (outputFormatter != null) {
            T retval = outputFormatter.parseContent(content, rootType);
            retval.validate();
            return retval;
        } else {
            String message = String.format("No Format for this content-type and data type : (%s, %s)",
                    type.toString(), rootType.getName());
            throw new UnsupportedFormatException(message);
        }
    }

    private <T extends CwmsDTOBase> List<T> parseContentListFromType(ContentType type, String content,
        Class<T> rootType) throws FormattingException {
        OutputFormatter outputFormatter = getOutputFormatterInternal(type, rootType);
        if (outputFormatter != null) {
            List<T> retval = outputFormatter.parseContentList(content, rootType);
            if (retval == null) {
                throw new UnsupportedFormatException("Cannot deserialize empty content array");
            }
            for (T obj : retval) {
                obj.validate();
            }
            return retval;
        } else {
            String message = String.format("No Format for this content-type and data type : (%s, %s)",
                type.toString(), rootType.getName());
            throw new UnsupportedFormatException(message);
        }
    }

    private OutputFormatter getOutputFormatterInternal(ContentType type,
                                               Class<? extends CwmsDTOBase> klass) {
        OutputFormatter outputFormatter = null;
        Map<Class<? extends CwmsDTOBase>, OutputFormatter> contentFormatters = formatters.get(type);
        if (contentFormatters != null && contentFormatters.containsKey(klass)) {
            outputFormatter = contentFormatters.get(klass);
        } else { // not in the list, look it up.
            FormattableWith[] annotationsByType = klass.getAnnotationsByType(FormattableWith.class);
            for (FormattableWith fw : annotationsByType) {
                ContentType fwCt = new ContentType(fw.contentType());
                if (type.equals(fwCt)) {
                    try {
                        outputFormatter = fw.formatter()
                                            .getDeclaredConstructor()
                                            .newInstance();
                        formatters.computeIfAbsent(type, k -> new HashMap<>())
                                  .put(klass,outputFormatter);
                    } catch (Exception ex) {
                        logger.atSevere().withCause(ex).log("Unable to create formatter.");
                        return null;
                    }
                }
            }
        }
        return outputFormatter;
    }

    /**
     * Retrieve the appropriate OutputFormatter for the given ContentType and DTO Class.
     * @param ct ContentType desired
     * @param klass CwmsDto
     * @return Appropriate formatter for the given ContentType and klass
     */
    public static OutputFormatter getOutputFormatter(ContentType ct, Class<? extends CwmsDTOBase> klass) {
        return formats.getOutputFormatterInternal(ct, klass);
    }

    /**
     * Retrieve the formatted output for the given ContentType and DTO Instance.
     * @param type ContentType Desired
     * @param toFormat Instance to format
     * @return String containing text of the DTO in the appropriate format
     * @throws FormattingException issues with Formatter lookup or formatting.
     */
    public static String format(ContentType type, CwmsDTOBase toFormat) throws FormattingException {
        return formats.getFormatted(type, toFormat);
    }

    /**
     * Retrieve the formatted output for the given ContentType and DTO Instances and a DTO Type.
     * @param type content type desired
     * @param toFormat list of objects to format
     * @param rootType DTO type of the list members
     * @return Formatted String
     * @throws FormattingException if the list of objects could not be converted.
     */
    public static String format(ContentType type, List<? extends CwmsDTOBase> toFormat,
            Class<? extends CwmsDTOBase> rootType) throws FormattingException {
        return formats.getFormatted(type, toFormat, rootType);
    }

    /**
     * Given a ContentType, text, and a given DTO type, parse the text and return an Object instance of the DTO type.
     * @param <T> DTO Type
     * @param type ContentType of the input
     * @param content data to parase
     * @param rootType expected DTO type
     * @return an instance of that DTO with the provided data.
     * @throws FormattingException any issues with lookup of parser or parsing.
     */
    public static <T extends CwmsDTOBase> T parseContent(ContentType type, String content, Class<T> rootType)
            throws FormattingException {
        return formats.parseContentFromType(type, content, rootType);
    }

    /**
     * Given a ContentType, text, and a given DTO type, parse the text and return an Object instance of the DTO type.
     * @param <T> DTO Type
     * @param type ContentType of the input.
     * @param inputStream source of data to parse.
     * @param rootType expected DTO type.
     * @return an instance of that DTO with the provided data.
     * @throws FormattingException any issues with lookup of parser or parsing.
     */
    public static <T extends CwmsDTOBase> T parseContent(ContentType type, InputStream inputStream, Class<T> rootType)
            throws FormattingException {
        return formats.parseContentFromType(type, inputStream, rootType);
    }

    /**
     * Given a ContentType, text, and a given DTO type, parse the text and return a list of Object instance of the DTO
     * type.
     * @param <T> DTO Type
     * @param type ContentType of the input
     * @param content data to parase
     * @param rootType expected DTO type
     * @return an instance of that DTO with the provided data.
     * @throws FormattingException any issues with lookup of parser or parsing.
     */
    public static <T extends CwmsDTOBase> List<T> parseContentList(ContentType type, String content, Class<T> rootType)
        throws FormattingException {
        return formats.parseContentListFromType(type, content, rootType);
    }

    /**
     * Parses the supplied header param and/or queryParam to determine the content type.
     * Query parameter takes priority over the header and is parsed the same way as the header
     * (i.e., supports full content types, versions, and DTO-specific aliases). If neither is
     * supplied an exception is thrown.
     *
     * @param header     Accept header value
     * @param queryParam format query parameter value
     * @param klass      DTO object class, used for identifying content type aliases from the DTO's
     *                   <code>FormattableWith</code> annotations.
     * @return an appropriate standard mimetype for lookup
     * @throws FormattingException if neither header nor queryParam can be parsed into a supported content type
     * @throws UnsupportedFormatException if preconditions aren't met or format is not supported.
     */
    public static ContentType parseHeaderAndQueryParm(String header, String queryParam, Class<? extends CwmsDTOBase> klass) {
        // If a query parameter is provided, it overrides the header.
        if (queryParam != null && !queryParam.isEmpty()) {
            ContentType ct = parseQueryParam(queryParam, klass);
            if (ct != null) {
                return ct;
            }
        }

        // No query parameter provided; use the header (parseHeader handles null/empty by mapping to */*)
        if (header == null) {
            throw new UnsupportedFormatException("no content type or format specified");
        }
        return parseHeader(header, klass);
    }

    /**
     * For endpoints that still allow either for transition, favors the query parameter as that's the likely user
     * expectation since machine systems wouldn't said both.
     * @param headerParam content type from a header
     * @param queryParam content type from a query parameter
     * @param klass DTO to find a matching formatter for.
     * @return ContentType appropriate to the given selection.
     * @throws UnsupportedFormatException if there is no matching content type for the given class
     */
    public static ContentType parseQueryOrHeaderParam(String headerParam, String queryParam,
        Class<? extends CwmsDTOBase> klass) {
        ContentType ct = null;
        if (!(queryParam == null || queryParam.isEmpty())) {
            ct = parseQueryParam(queryParam, klass);
        } else if (headerParam != null) {
            ct = parseHeader(headerParam, klass);
        } else {
            ct = parseHeader(DEFAULT, klass);
        }
        if (ct == null) {
            throw new UnsupportedFormatException("Content-Type " + (headerParam == null ? queryParam : headerParam)
                + " is not available.");
        }
        return ct;
    }

    /**
     * Given the ContentType provided in a queryParameter extract and convert to a ContentType issue.
     * @param queryParam value of the "format" query parameter.
     * @param klass type of DTO expected.
     * @return instance of ContentType
     */
    public static ContentType parseQueryParam(String queryParam, Class<? extends CwmsDTOBase> klass) {
        ContentTypeAliasMap aliasMap = ContentTypeAliasMap.empty();
        if (klass != null) {
            aliasMap = ContentTypeAliasMap.forDtoClass(klass);
        }

        ContentType retVal = null;
        if (queryParam != null && !queryParam.isEmpty()) {
            String val = typeMap.get(queryParam);
            if (val != null) {
                retVal = aliasMap.getContentType(val);
                if (retVal == null) {
                    retVal = new ContentType(val);
                }
            }
        }

        return retVal;
    }

    /**
     * Parses the supplied header param to determine the content type.
     *
     * @param header Accept header value. If null, will assume &#42;&#47;&#42; content type
     * @param klass  DTO object class, used for identifying content type aliases from the DTO's
     *               {@link cwms.cda.formatters.annotations.FormattableWith} annotations.
     * @return an appropriate standard mimetype for lookup
     * @throws FormattingException if the header can't be identified as a mimetype
     * @throws UnsupportedFormatException if header is invalid or for a format that is not supported.
     */
    public static @NotNull ContentType parseHeader(@Nullable String header,
        @NotNull Class<? extends CwmsDTOBase> klass) {
        Objects.requireNonNull(klass, "Cannot determine content type without a DTO class definition");
        ContentTypeAliasMap aliasMap = ContentTypeAliasMap.forDtoClass(klass);
        //Swap out null content type with */* for flexibility.
        //This routine will match DTO's when the DEFAULT alias specified by the format annotations.
        if (header == null || header.trim().isEmpty()) {
            header = DEFAULT;
        }
        //TreeSet will sort based on prioritized content type
        //if multiple valid content types are specified in the header.
        SortedSet<ContentType> contentTypes = new TreeSet<>();
        String[] all = header.split(",");
        logger.atFinest().log("Finding handlers %d", all.length);
        for (String ct : all) {
            ContentType aliasType = aliasMap.getContentType(ct);
            //Found type defined in annotations, add to the priority list.
            if (aliasType != null) {
                logger.atFinest().log("%s converted to %s", ct, aliasType);
                contentTypes.add(aliasType);
            } else {
                //If the DTO parameter is null, alias map is empty. Compare against well-known types
                //Only use the ContentType classes initialized in contentTypeList rather than
                //the client headers itself
                ContentType type = new ContentType(ct);
                if (contentTypeList.contains(type)) {
                    contentTypes.add(type);
                }
            }
        }
        logger.atFinest().log("have %d", contentTypes.size());
        //Look through known content types to match using priority sorted TreeSet
        for (ContentType ct : contentTypes) {
            logger.atFinest().log("checking %s", ct.toString());
            if (contentTypeList.contains(ct)) {
                return ct;
            }
        }
        throw new UnsupportedFormatException("Format header " + header + " could not be parsed");
    }
}
