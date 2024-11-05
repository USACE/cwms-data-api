package cwms.cda.formatters;

import java.io.InputStream;
import java.util.List;

import cwms.cda.data.dto.CwmsDTOBase;

public interface OutputFormatter {
    String DESERIALIZE_CONTENT_MESSAGE = "Could not deserialize: %s of type: %s";
    String UNSUPPORTED_MESSAGE = "Unable to process your request. Deserialization of %s not yet supported.";
    String getContentType();
    String format(CwmsDTOBase dto);
    String format(List<? extends CwmsDTOBase> dtoList);
    default <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_MESSAGE, getContentType()));
    }
    default <T extends CwmsDTOBase> List<T> parseContentList(String content, Class<T> type) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_MESSAGE, getContentType()));
    }
    default <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_MESSAGE, getContentType()));
    }
}
