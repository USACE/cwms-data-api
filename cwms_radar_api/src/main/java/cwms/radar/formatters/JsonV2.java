package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.Office;
import io.javalin.plugin.json.JavalinJson;
import service.annotations.FormatService;

@FormatService(contentType = "application/json;version=2", dataTypes = {Office.class,Location.class})
public class JsonV2 implements OutputFormatter {

	@Override
	public String getContentType() {		
		return "appliation/json;version=2";
	}

	@Override
	public String format(CwmsDTO dto) {		
		return JavalinJson.toJson(dto);
	}

	@Override
	public String format(List<? extends CwmsDTO> dtoList) {		
		return JavalinJson.toJson(dtoList);
	}

}
