package cwms.radar.formatters.tab;

import java.util.List;

import cwms.radar.data.dao.CwmsDao;
import cwms.radar.data.dao.Office;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;

public class TabV1 implements OutputFormatter {

    @Override
    public String getContentType() {        
        return Formats.TAB;
    }

    @Override
    public String format(CwmsDao dao) {
        if( dao instanceof Office ){
            return new TabV1Office().format(dao);
        } else {
            return null;
        }        
    }

    @Override
    public String format(List<? extends CwmsDao> daoList) {
        if( !daoList.isEmpty() && daoList.get(0) instanceof Office ){
            return new TabV1Office().format(daoList);
        } else {
            return null;
        }        
    }

    
    
}
