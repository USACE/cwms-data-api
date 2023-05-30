package cwms.radar.formatters.tab;

import java.util.List;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.CwmsDTOBase;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import service.annotations.FormatService;

@FormatService(contentType = Formats.TAB, dataTypes = {Office.class})
public class TabV1 implements OutputFormatter {

    @Override
    public String getContentType() {
        return Formats.TAB;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        if( dto instanceof Office ){
            return new TabV1Office().format(dto);
        } else {
            return null;
        }
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        if( !dtoList.isEmpty() && dtoList.get(0) instanceof Office ){
            return new TabV1Office().format(dtoList);
        } else {
            return null;
        }
    }



}
