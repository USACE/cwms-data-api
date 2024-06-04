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

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.annotations.FormattableWith;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Formats {
    public static final Logger logger = Logger.getLogger(Formats.class.getName());
    public static final String PLAIN = "text/plain";    // Only used as a constant, not for any
    // data mapping
    public static final String JSON = "application/json";
    public static final String XML = "application/xml";
    public static final String XMLV2 = "application/xml;version=2";
    public static final String WML2 = "application/vnd.opengis.waterml+xml";
    public static final String JSONV2 = "application/json;version=2";
    public static final String TAB = "text/tab-separated-values";
    public static final String CSV = "text/csv";
    public static final String GEOJSON = "application/geo+json";
    public static final String PGJSON = "application/vnd.pg+json";
    public static final String NAMED_PGJSON = "application/vnd.named+pg+json";
    public static final String DEFAULT = "*/*";


    private static final List<ContentType> contentTypeList = new ArrayList<>();

    static {
        contentTypeList.addAll(
                Stream.of(JSON, XML, XMLV2, WML2, JSONV2, TAB, CSV, GEOJSON, PGJSON, NAMED_PGJSON)
                        .map(ContentType::new)
                        .collect(Collectors.toList()));
    }

    private static final Map<String, String> typeMap = new LinkedHashMap<>();

    static {
        typeMap.put("json", Formats.JSON);
        typeMap.put("xml", Formats.XML);
        typeMap.put("wml2", Formats.WML2);
        typeMap.put("tab", Formats.TAB);
        typeMap.put("csv", Formats.CSV);
        typeMap.put("geojson", Formats.GEOJSON);
        typeMap.put("pgjson", Formats.PGJSON);
        typeMap.put("named-pgjson", Formats.NAMED_PGJSON);
    }


    private final Map<ContentType, Map<Class<? extends CwmsDTOBase>, OutputFormatter>> formatters = new LinkedHashMap<>();

    private static final Formats formats = new Formats();

    private Formats() {
    }


    private String getFormatted(ContentType type, CwmsDTOBase toFormat) throws FormattingException {
        Objects.requireNonNull(toFormat, "Object to be formatted should not be null");
        formatters.keySet().forEach(k -> logger.fine(k::toString));
        OutputFormatter outputFormatter = getOutputFormatter(type, toFormat.getClass());

        if (outputFormatter != null) {
            return outputFormatter.format(toFormat);
        } else {
            String message = String.format("No Format for this content-type and data-type : (%s, %s)",
                    type.toString(), toFormat.getClass().getName());
            throw new FormattingException(message);
        }

    }

    private String getFormatted(ContentType type, List<? extends CwmsDTOBase> dtos, Class<?
            extends CwmsDTOBase> rootType) throws FormattingException {
        for (ContentType key : formatters.keySet()) {
            logger.finest(() -> key.toString());
        }

        OutputFormatter outputFormatter = getOutputFormatter(type, rootType);

        if (outputFormatter != null) {
            return outputFormatter.format(dtos);
        } else {
            String message = String.format("No Format for this content-type and data type : (%s, %s)",
                    type.toString(), dtos.get(0).getClass().getName());
            throw new FormattingException(message);
        }
    }

    private <T extends CwmsDTOBase> T parseContentFromType(ContentType type, String content, Class<T> rootType)
            throws FormattingException {
        OutputFormatter outputFormatter = getOutputFormatter(type, rootType);
        if (outputFormatter != null) {
            T retval = outputFormatter.parseContent(content, rootType);
            retval.validate();
            return retval;
        } else {
            String message = String.format("No Format for this content-type and data type : (%s, %s)",
                    type.toString(), rootType.getName());
            throw new FormattingException(message);
        }
    }

    private OutputFormatter getOutputFormatter(ContentType type,
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
                        logger.log(Level.SEVERE, "Unable to create formatter.", ex);
                        return null;
                    }
                }
            }
        }
        return outputFormatter;
    }

    public static String format(ContentType type, CwmsDTOBase toFormat) throws FormattingException {
        return formats.getFormatted(type, toFormat);
    }

    public static String format(ContentType type, List<? extends CwmsDTOBase> toFormat, Class<?
            extends CwmsDTOBase> rootType) throws FormattingException {
        return formats.getFormatted(type, toFormat, rootType);
    }

    public static <T extends CwmsDTOBase> T parseContent(ContentType type, String content, Class<T> rootType)
            throws FormattingException {
        return formats.parseContentFromType(type, content, rootType);
    }

    /**
     * Parses the supplied header param or queryParam to determine the content type.
     * If both are supplied an exception is thrown.  If neither are supplied an exception is thrown.
     *
     * @param header     Accept header value
     * @param queryParam format query parameter value
     * @return an appropriate standard mimetype for lookup
     * @throws FormattingException if the header and queryParam are both supplied or neither are
     */
    public static ContentType parseHeaderAndQueryParm(String header, String queryParam) {
        if (queryParam != null && !queryParam.isEmpty()) {
            if (header != null && !header.isEmpty() && !DEFAULT.equals(header.trim())) {
                // If the user supplies an accept header and also a format= parameter, which
                // should we use?
                // The older format= query parameters don't give us the option to supply a
                // version the
                // way that the accept header does.
                throw new FormattingException("Accept header and query parameter are both "
                        + "present, this is not supported.");
            }

            ContentType ct = parseQueryParam(queryParam);
            if (ct != null) {
                return ct;
            } else {
                throw new FormattingException("content-type " + queryParam + " is not implemented");
            }
        } else if (header == null) {
            throw new FormattingException("no content type or format specified");
        } else {
            ContentType ct = parseHeader(header);
            if (ct != null) {
                return ct;
            }
        }
        throw new FormattingException("Content-Type " + header + " is not available");
    }


    public static ContentType parseQueryParam(String queryParam) {
        ContentType retVal = null;
        if (queryParam != null && !queryParam.isEmpty()) {
            String val = typeMap.get(queryParam);
            if (val != null) {
                retVal = new ContentType(val);
            }
        }

        return retVal;
    }


    public static ContentType parseHeader(String header) {
        return parseHeader(header, new ContentTypeAliasMap());
    }
    public static ContentType parseHeader(String header, ContentTypeAliasMap aliasMap) {
        ArrayList<ContentType> contentTypes = new ArrayList<>();

        if (header != null && !header.isEmpty()) {
            String[] all = header.split(",");
            logger.log(Level.FINEST, "Finding handlers {0}", all.length);
            for (String ct : all) {
                ContentType aliasType = aliasMap.getContentType(ct);
                if (aliasType != null)
                {
                    logger.finest(() -> ct + " converted to " + aliasType);
                    contentTypes.add(aliasType);
                }
                else
                {
                    logger.finest(ct);
                    contentTypes.add(new ContentType(ct));
                }
            }
            Collections.sort(contentTypes);
        }
        logger.finest(() -> "have " + contentTypes.size());
        for (ContentType ct : contentTypes) {
            logger.finest(() -> "checking " + ct.toString());
            if (contentTypeList.contains(ct)) {
                return ct;
            }
        }
        for (ContentType ct : contentTypes) {
            if (ct.getType().equals(DEFAULT)) {
                return new ContentType(Formats.JSON);
            }
        }
        return null;
    }
}
