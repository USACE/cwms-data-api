package cwms.cda.formatters;

import java.io.InputStream;
import java.util.List;

import cwms.cda.data.dto.CwmsDTOBase;

public interface OutputFormatter {
    String getContentType();
    String format(CwmsDTOBase dto);
    String format(List<? extends CwmsDTOBase> dtoList);
    default <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        throw new UnsupportedOperationException("Unable to process your request. Deserialization of "
                + getContentType() + " not yet supported.");
    }
    default <T extends CwmsDTOBase> List<T> parseContentList(String content, Class<T> type) {
        throw new UnsupportedOperationException("Unable to process your request. Deserialization of "
            + getContentType() + " not yet supported.");
    }
    default <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        throw new UnsupportedOperationException("Unable to process your request. Deserialization of "
                + getContentType() + " not yet supported.");
    }
}
