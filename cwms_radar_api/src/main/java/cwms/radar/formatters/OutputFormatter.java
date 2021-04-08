package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dao.CwmsDao;

public interface OutputFormatter {
    public String getContentType();
    public String format(CwmsDao dao);
    public String format(List<? extends CwmsDao> daoList);
}
