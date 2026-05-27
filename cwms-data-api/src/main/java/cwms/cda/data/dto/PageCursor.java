package cwms.cda.data.dto;

import java.util.Base64.Encoder;

public interface PageCursor {
    /**
     * Decodes the provided cursor string using the specified delimiter and sets the appropriate fields in the implementing class.
     * @param cursor the encoded cursor string to decode
     * @param delimiter the delimiter used to separate fields in the encoded cursor string. By default, this is ||
     */
    void decodeCursor(String cursor, String delimiter);

    /**
     * Encodes the fields of the implementing class into a cursor string using the specified encoder and delimiter.
     * @param encoder the Base64 Encoder to use for encoding the cursor string
     * @param delimiter the delimiter used to separate fields in the encoded cursor string. By default, this is ||
     * @return the encoded cursor string representing the current state of the implementing class's fields
     */
    String encode(Encoder encoder, String delimiter);

    /**
     * Encodes a field that may be null into a string representation. If the field is null, it returns the string "null". Otherwise, it returns the string representation of the field.
     * @param field the field to encode, which may be null
     * @return a string representation of the field, where null is represented as "null"
     */
    static String encodeNullableField(Object field) {
        return field == null ? "null" : field.toString();
    }

    /**
     * Decodes a string representation of a field that may be null. If the input string is "null", it returns null. Otherwise, it returns the input string as is.
     * @param field the string representation of the field to decode, where "null" represents a null value
     * @return the decoded field, which is null if the input string is "null", or the input string itself if it is not "null". The caller is responsible for converting the string to the appropriate type if necessary.
     */
    static String decodeNullableField(String field) {
        return "null".equals(field) ? null : field;
    }
}
