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

package cwms.radar.formatters;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.CwmsDTOBase;
import cwms.radar.helpers.ResourceHelper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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


    private static List<ContentType> contentTypeList = new ArrayList<>();

    static {
        contentTypeList.addAll(
                Stream.of(JSON, XML, XMLV2, WML2, JSONV2, TAB, CSV, GEOJSON, PGJSON, NAMED_PGJSON)
                        .map(ContentType::new)
                        .collect(Collectors.toList()));
    }

    private static Map<String, String> typeMap = new LinkedHashMap<>();

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


    private Map<ContentType, Map<Class<CwmsDTO>, OutputFormatter>> formatters = null;

    private static Formats formats = null;

    private Formats() throws IOException {
        formatters = new LinkedHashMap<>();
        InputStream formatList = ResourceHelper.getResourceAsStream("/formats.list",
                this.getClass());
        BufferedReader br = new BufferedReader(new InputStreamReader(formatList));
        while (br.ready()) {
            String line = br.readLine();
            logger.finest(line);
            String[] typeFormatterClasses = line.split(":");

            ContentType type = new ContentType(typeFormatterClasses[0]);
            logger.finest("Adding links for content-type: " + type);

            try {
                @SuppressWarnings("unchecked")
                Class<OutputFormatter> formatter =
                        (Class<OutputFormatter>) Class.forName(typeFormatterClasses[1]);
                OutputFormatter formatterInstance;
                logger.finest("Formatter class: " + typeFormatterClasses[1]);
                formatterInstance = formatter.getDeclaredConstructor().newInstance();
                Map<Class<CwmsDTO>, OutputFormatter> tmp = new HashMap<>();

                for (String clazz : typeFormatterClasses[2].split(";")) {
                    logger.finest("\tFor Class: " + clazz);

                    @SuppressWarnings("unchecked")
                    Class<CwmsDTO> formatForClass = (Class<CwmsDTO>) Class.forName(clazz);
                    tmp.put(formatForClass, formatterInstance);
                }

                formatters.put(type, tmp);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                     | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                throw new IOException("Failed to load format list, formatter for "
                        + typeFormatterClasses[0] + " point to a class with an invalid constructor", e);
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to find class referenced for formatter "
                        + typeFormatterClasses[0], e);
            }
        }

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

    private OutputFormatter getOutputFormatter(ContentType type, Class<? extends CwmsDTOBase> klass) {
        OutputFormatter outputFormatter = null;
        Map<Class<CwmsDTO>, OutputFormatter> contentFormatters = formatters.get(type);
        if (contentFormatters != null) {
            outputFormatter = contentFormatters.get(klass);
        }
        return outputFormatter;
    }

    private String getFormatted(ContentType type, List<? extends CwmsDTOBase> dtos, Class<?
            extends CwmsDTOBase> rootType) throws FormattingException {
        for (ContentType key : formatters.keySet()) {
            logger.finest(key.toString());
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

    private static void init() {
        if (formats == null) {
            logger.finest("creating instance");
            try {
                formats = new Formats();
            } catch (IOException err) {
                throw new FormattingException("Failed to load format map", err);
            }
        }
    }

    public static String format(ContentType type, CwmsDTOBase toFormat) throws FormattingException {
        logger.finest("formats");
        init();
        return formats.getFormatted(type, toFormat);
    }

    public static String format(ContentType type, List<? extends CwmsDTOBase> toFormat, Class<?
            extends CwmsDTOBase> rootType) throws FormattingException {
        logger.finest("format list");
        init();
        return formats.getFormatted(type, toFormat, rootType);
    }


    /**
     * Parses the supplied header param or queryParam to determine the content type.
     * If both are supplied an exception is thrown.  If neither are supplied an exception is thrown.
     *
     * @param header     Accept header value
     * @param queryParam format query parameter value
     * @return an appropriate standard mimetype for lookup
     */
    public static ContentType parseHeaderAndQueryParm(String header, String queryParam) {
        if (queryParam != null && !queryParam.isEmpty()) {
            if (header != null && !header.isEmpty() && !"*/*".equals(header.trim())) {
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
        ContentType retval = null;
        if (queryParam != null && !queryParam.isEmpty()) {
            String val = typeMap.get(queryParam);
            if (val != null) {
                retval = new ContentType(val);
            }
        }

        return retval;
    }


    public static ContentType parseHeader(String header) {
        ArrayList<ContentType> contentTypes = new ArrayList<>();

        if (header != null && !header.isEmpty()) {
            String[] all = header.split(",");
            logger.finest("Finding handlers " + all.length);
            for (String ct : all) {
                logger.finest(ct);
                contentTypes.add(new ContentType(ct));
            }
            Collections.sort(contentTypes);
        }
        logger.finest("have " + contentTypes.size());
        for (ContentType ct : contentTypes) {
            logger.finest("checking " + ct.toString());
            if (contentTypeList.contains(ct)) {
                return ct;
            }
        }
        for (ContentType ct : contentTypes) {
            if (ct.getType().equals("*/*")) {
                return new ContentType(Formats.JSON);
            }
        }
        return null;
    }
}
