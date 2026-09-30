/*
 *
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static cwms.cda.formatters.Formats.MULTIPART_FORM_DATA;

import cwms.cda.formatters.FormattingException;
import io.javalin.http.Context;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class MultipartParser {

    private MultipartParser() {
        throw new UnsupportedOperationException("Cannot instantiate utility class MultipartParser");
    }

    /**
     * Parse multipart form data provided in the given context.
     * @param ctx the web request context
     * @return ParsedMultipart the parsed multipart form data
     */
    public static ParsedMultipart parseMultipart(Context ctx) {
        String contentType = ctx.req.getContentType();
        if (contentType == null || !contentType.toLowerCase(Locale.ROOT).startsWith(MULTIPART_FORM_DATA)) {
            return new ParsedMultipart(Map.of(), null);
        }

        String boundaryToken = null;
        for (String part : contentType.split(";")) {
            String token = part.trim();
            if (token.toLowerCase(Locale.ROOT).startsWith("boundary=")) {
                boundaryToken = token.substring("boundary=".length());
                break;
            }
        }

        if (boundaryToken == null || boundaryToken.isBlank()) {
            throw new FormattingException("Unable to parse multipart form data: missing boundary");
        }

        String boundary = boundaryToken;
        if (boundary.startsWith("\"") && boundary.endsWith("\"") && boundary.length() > 1) {
            boundary = boundary.substring(1, boundary.length() - 1);
        }

        try {
            byte[] bodyBytes = ctx.bodyAsInputStream().readAllBytes();
            return parseMultipartBody(bodyBytes, boundary);
        } catch (IOException e) {
            throw new FormattingException("Unable to parse multipart form data", e);
        }
    }

    /**
     * Read the multipart value from the given context.
     * @param ctx the web request context
     * @return byte[] the value as a byte array
     */
    public static byte[] readMultipartValue(Context ctx) {
        String valueText = ctx.formParam("value");
        if (valueText != null) {
            return valueText.getBytes(StandardCharsets.UTF_8);
        }
        return new byte[0];
    }

    private static ParsedMultipart parseMultipartBody(byte[] bodyBytes, String boundary) {
        String payload = new String(bodyBytes, StandardCharsets.ISO_8859_1);
        String delimiter = "--" + boundary;
        String[] segments = payload.split(java.util.regex.Pattern.quote(delimiter));

        Map<String, String> fields = new HashMap<>();
        byte[] value = null;

        for (String rawSegment : segments) {
            String segment = normalizeSegment(rawSegment);
            if (segment != null) {
                PartData part = parsePartData(segment);
                if (part != null && part.name != null) {
                    byte[] partBytes = part.body.getBytes(StandardCharsets.ISO_8859_1);
                    if (isValuePart(part.name, part.filePart)) {
                        value = partBytes;
                    } else {
                        fields.put(part.name, new String(partBytes, StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return new ParsedMultipart(fields, value);
    }

    private static String normalizeSegment(String rawSegment) {
        if (rawSegment == null || rawSegment.isBlank() || rawSegment.startsWith("--")) {
            return null;
        }

        if (rawSegment.startsWith("\r\n")) {
            return rawSegment.substring(2);
        }
        return rawSegment;
    }

    private static PartData parsePartData(String segment) {
        int headerSeparator = segment.indexOf("\r\n\r\n");
        if (headerSeparator < 0) {
            return null;
        }

        String headers = segment.substring(0, headerSeparator);
        String body = segment.substring(headerSeparator + 4);
        if (body.endsWith("\r\n")) {
            body = body.substring(0, body.length() - 2);
        }

        String name = extractPartName(headers);
        boolean filePart = hasFileName(headers);
        return new PartData(name, filePart, body);
    }

    private static String extractPartName(String headers) {
        for (String headerLine : headers.split("\r\n")) {
            String lower = headerLine.toLowerCase(Locale.ROOT);
            if (!lower.startsWith("content-disposition:")) {
                continue;
            }

            for (String dispositionPart : headerLine.split(";")) {
                String trimmed = dispositionPart.trim();
                if (trimmed.startsWith("name=")) {
                    return stripQuotes(trimmed.substring(5));
                }
            }
        }

        return null;
    }

    private static boolean hasFileName(String headers) {
        for (String headerLine : headers.split("\r\n")) {
            String lower = headerLine.toLowerCase(Locale.ROOT);
            if (!lower.startsWith("content-disposition:")) {
                continue;
            }

            for (String dispositionPart : headerLine.split(";")) {
                if (dispositionPart.trim().startsWith("filename=")) {
                    return true;
                }
            }
        }

        return false;
    }

    private static boolean isValuePart(String name, boolean filePart) {
        return filePart || "value".equalsIgnoreCase(name)
            || "blob".equalsIgnoreCase(name)
            || "file".equalsIgnoreCase(name)
            || "content".equalsIgnoreCase(name);
    }

    private static String stripQuotes(String input) {
        if (input == null) {
            return null;
        }

        String value = input.trim();
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() > 1) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }



    public static final class ParsedMultipart {
        private final Map<String, String> fields;
        private final byte[] value;

        ParsedMultipart(Map<String, String> fields, byte[] value) {
            this.fields = fields;
            this.value = value;
        }

        public String field(String name) {
            return fields.get(name);
        }

        public byte[] value() {
            return value;
        }
    }

    private static final class PartData {
        private final String name;
        private final boolean filePart;
        private final String body;

        PartData(String name, boolean filePart, String body) {
            this.name = name;
            this.filePart = filePart;
            this.body = body;
        }
    }
}
