package cwms.cda.formatters.tab;

import java.util.List;

import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OutputFormatter;

public class TabV1 implements OutputFormatter {

    @Override
    public String getContentType() {
        return Formats.TAB;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        if (dto instanceof Office ) {
            return new TabV1Office().format(dto);
        } else {
            return null;
        }
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        if (!dtoList.isEmpty() && dtoList.get(0) instanceof Office ) {
            return new TabV1Office().format(dtoList);
        } else {
            return null;
        }
    }
}
