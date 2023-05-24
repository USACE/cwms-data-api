package cwms.radar.formatters.csv;

import java.util.List;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.CwmsDTOBase;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import service.annotations.FormatService;

@FormatService(contentType = Formats.CSV, dataTypes = {Office.class, LocationGroup.class})
public class CsvV1 implements OutputFormatter {

    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        String retval = null;
        if( dto instanceof Office )
        {
            retval = new CsvV1Office().format(dto);
        } else if( dto instanceof LocationGroup ){
            retval = new CsvV1LocationGroup().format(dto);
        }
        return retval;
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        String retval = null;
        if(dtoList != null && !dtoList.isEmpty())
        {
            CwmsDTOBase dto = dtoList.get(0);
            if( dto instanceof Office)
            {
                retval = new CsvV1Office().format(dtoList);
            }
            else if(dto instanceof LocationGroup)
            {
                retval = new CsvV1LocationGroup().format(dtoList);
            }

        }
        return retval;
    }



}
