package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dto.CwmsDTOBase;

public interface OutputFormatter {
    public String getContentType();
    public String format(CwmsDTOBase dto);
    public String format(List<? extends CwmsDTOBase> dtoList);
}
