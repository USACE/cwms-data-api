package cwms.radar.formatters.csv;

import java.util.List;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import service.annotations.FormatService;

@FormatService(contentType = Formats.CSV, dataTypes = {Office.class})
public class CsvV1 implements OutputFormatter {

    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    @Override
    public String format(CwmsDTO dto) {
        if( dto instanceof Office ){
            return new CsvV1Office().format(dto);
        } else {
            return null;
        }
    }

    @Override
    public String format(List<? extends CwmsDTO> dtoList) {
        if( !dtoList.isEmpty() && dtoList.get(0) instanceof Office ){
            return new CsvV1Office().format(dtoList);
        } else {
            return null;
        }
    }



}
