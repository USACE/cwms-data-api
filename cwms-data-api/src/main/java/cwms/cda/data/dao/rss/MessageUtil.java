/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.data.dao.rss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.formatters.json.JsonV2;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.jooq.exception.DataAccessException;

final class MessageUtil {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final ObjectMapper MAPPER = JsonV2.buildObjectMapper();

    private static String extractTextMessage(Object userData) {
        try {
            Struct struct = (Struct) userData;
            Object[] attrs = struct.getAttributes();

            for (Object attr : attrs) {
                if (attr instanceof String ) {
                    return (String) attr;
                }
                if (attr instanceof Clob) {
                    return clobToString((Clob) attr);
                }
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeException("Error reading TEXT_MESSAGE", e);
        }
    }

    private static String clobToString(Clob clob) {
        try (Reader r = clob.getCharacterStream();
             StringWriter w = new StringWriter()) {
            r.transferTo(w);
            return w.toString();
        } catch (Exception e) {
            throw new RuntimeException("Error reading CLOB", e);
        }
    }

    private static Map<?, ?> extractMapMessage(Object userData) {
        try {
            Struct struct = (Struct) userData;
            Object[] attrs = struct.getAttributes();
            for (Object attr : attrs) {
                if (attr instanceof byte[]) {
                    try(var stream = new ObjectInputStream(new ByteArrayInputStream((byte[]) attr))) {
                        stream.setObjectInputFilter(MessageUtil::objectInputFilter);
                        Object deserialized = stream.readObject();
                        if(deserialized instanceof Map) {
                            //noinspection rawtypes
                            return (Map) deserialized;
                        }
                    }
                }
            }
            return Map.of();
        } catch (SQLException | IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error reading MAP_MESSAGE", e);
        }
    }

    static Optional<String> extractPayload(Object userData) {
        if (userData == null) {
            return Optional.empty();
        }
        try {
            Struct struct = (Struct) userData;
            String oracleType = struct.getSQLTypeName();
            if (oracleType.endsWith("JMS_TEXT_MESSAGE")) {
                return Optional.ofNullable(extractTextMessage(userData));
            }
            if (oracleType.endsWith("JMS_MAP_MESSAGE")) {
                Map<?, ?> map = extractMapMessage(userData);
                return Optional.ofNullable(MAPPER.writeValueAsString(map));   // JACKSON HERE
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DataAccessException("Error extracting message payload", e);
        } catch (JsonProcessingException e) {
            LOGGER.atWarning().withCause(e).log("Error extracting JMS payload", e);
            return Optional.empty();
        }
    }


    private static ObjectInputFilter.Status objectInputFilter(ObjectInputFilter.FilterInfo info) {
        if (info.depth() > 10) {
            return ObjectInputFilter.Status.REJECTED;
        }
        if (info.references() > 10_000) {
            return ObjectInputFilter.Status.REJECTED;
        }
        if (info.arrayLength() >= 0 && info.arrayLength() > 1_000_000) {
            return ObjectInputFilter.Status.REJECTED;
        }
        Class<?> c = info.serialClass();
        if (c == null) {
            return ObjectInputFilter.Status.UNDECIDED;
        }
// Only allow Maps + Strings
        if (c == String.class) {
            return ObjectInputFilter.Status.ALLOWED;
        }
        if (c == HashMap.class
            || c == LinkedHashMap.class
            || c == Map.Entry.class
            || c == Map.Entry[].class) {
            return ObjectInputFilter.Status.ALLOWED;
        }
        if (c == Integer.class || c == Long.class || c == Double.class ||
            c == Boolean.class || c == Short.class || c == Byte.class ||
            c == Float.class || c == Character.class || c == Number.class) {
            return ObjectInputFilter.Status.ALLOWED;
        }
        return ObjectInputFilter.Status.REJECTED;
    }
}
