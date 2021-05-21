package cwms.radar.formatters.csv;

import java.util.List;

import cwms.radar.data.dao.CwmsDao;
import cwms.radar.data.dao.Office;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import service.annotations.FormatService;

@FormatService(contentType = "text/csv", dataTypes = {Office.class})
public class CsvV1 implements OutputFormatter {

    @Override
    public String getContentType() {        
        return Formats.TAB;
    }

    @Override
    public String format(CwmsDao dao) {
        if( dao instanceof Office ){
            return new CsvV1Office().format(dao);
        } else {
            return null;
        }        
    }

    @Override
    public String format(List<? extends CwmsDao> daoList) {
        if( !daoList.isEmpty() && daoList.get(0) instanceof Office ){
            return new CsvV1Office().format(daoList);
        } else {
            return null;
        }        
    }

    
    
}
