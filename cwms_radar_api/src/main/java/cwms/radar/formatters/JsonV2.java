package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dao.CwmsDao;
import io.javalin.plugin.json.JavalinJson;

public class JsonV2 implements OutputFormatter {

	@Override
	public String getContentType() {		
		return "appliation/json;version=2";
	}

	@Override
	public String format(CwmsDao dao) {		
		return JavalinJson.toJson(dao);
	}

	@Override
	public String format(List<? extends CwmsDao> daoList) {		
		return JavalinJson.toJson(daoList);
	}

}
