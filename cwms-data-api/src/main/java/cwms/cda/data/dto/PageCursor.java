package cwms.cda.data.dto;

import java.util.Base64;

public interface PageCursor {
    void decodeCursor(String cursor, String delimiter);
    String encode(Base64.Encoder encoder, String delimiter);
    static String encodeNullableField(Object field) {
        return field == null ? "null" : field.toString();
    }
    static String decodeNullableField(String field) {
        return "null".equals(field) ? null : field;
    }
}
