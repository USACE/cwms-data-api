package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dto.CwmsDTO;

public interface OutputFormatter {
    public String getContentType();
    public String format(CwmsDTO dto);
    public String format(List<? extends CwmsDTO> dtoList);
}
