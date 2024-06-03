package cwms.cda.formatters;

import java.util.List;

import cwms.cda.data.dto.CwmsDTOBase;

public interface OutputFormatter {
    String getContentType();
    String format(CwmsDTOBase dto);
    String format(List<? extends CwmsDTOBase> dtoList);
    <T extends CwmsDTOBase> T parseContent(String content, Class<T> type);
}
