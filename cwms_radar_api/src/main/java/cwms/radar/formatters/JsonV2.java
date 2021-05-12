package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dao.CwmsDao;
import cwms.radar.data.dao.Location;
import cwms.radar.data.dao.Office;
import io.javalin.plugin.json.JavalinJson;
import service.annotations.FormatService;

@FormatService(contentType = "application/json;version=2", dataTypes = {Office.class,Location.class})
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
