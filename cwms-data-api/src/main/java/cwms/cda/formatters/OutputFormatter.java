package cwms.cda.formatters;

import cwms.cda.data.dto.CwmsDTOBase;
import java.io.InputStream;
import java.util.List;


public interface OutputFormatter {
    String DESERIALIZE_CONTENT_MESSAGE = "Could not deserialize: %s of type: %s";
    String UNSUPPORTED_MESSAGE = "Unable to process your request. Deserialization of %s not yet supported.";

    String getContentType();

    String format(CwmsDTOBase dto);

    String format(List<? extends CwmsDTOBase> dtoList);

    default <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_MESSAGE, getContentType()));
    }

    default <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_MESSAGE, getContentType()));
    }

    default <T extends CwmsDTOBase> List<T> parseContentList(String content, Class<T> type) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_MESSAGE, getContentType()));
    }

    /**
     * Used where more advanced handling is required.
     * DUMMY
     */
    public class DUMMY implements OutputFormatter {

        @Override
        public String getContentType() {
            throw new UnsupportedOperationException("Unimplemented method 'getContentType'");
        }

        @Override
        public String format(CwmsDTOBase dto) {
            throw new UnsupportedOperationException("Unimplemented method 'format'");
        }

        @Override
        public String format(List<? extends CwmsDTOBase> dtoList) {
            throw new UnsupportedOperationException("Unimplemented method 'format'");
        }
    }
}
