package cwms.radar.formatters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.Office;
import io.javalin.http.BadRequestResponse;
import io.javalin.plugin.json.JavalinJson;
import service.annotations.FormatService;

@FormatService(contentType = Formats.JSON, dataTypes = {Office.class,Location.class})
public class JsonV1 implements OutputFormatter{

	@Override
	public String getContentType() {
		return Formats.JSON;
	}

	@Override
	public String format(CwmsDTO dto) {
		OfficeFormatV1 fmtv1 = new OfficeFormatV1(Arrays.asList((Office)dto) );		
		return JavalinJson.toJson(fmtv1);
	}

	@Override
	public String format(List<? extends CwmsDTO> daoList) {
        if( daoList.get(0) instanceof Office ){
            OfficeFormatV1 fmtv1 = new OfficeFormatV1(
                daoList.stream().map( office -> (Office)office ).collect(Collectors.toList())
            );
            return JavalinJson.toJson(fmtv1);
        } else {
			throw new BadRequestResponse(String.format("Format {} not implemented for this data type"));
		}
	}

}
