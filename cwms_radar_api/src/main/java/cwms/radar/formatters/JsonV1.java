package cwms.radar.formatters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import cwms.radar.data.dao.CwmsDao;
import cwms.radar.data.dao.Office;
import io.javalin.http.BadRequestResponse;
import io.javalin.plugin.json.JavalinJson;

public class JsonV1 implements OutputFormatter{

	@Override
	public String getContentType() {
		return "application/json";
	}

	@Override
	public String format(CwmsDao dao) {
		OfficeFormatV1 fmtv1 = new OfficeFormatV1(Arrays.asList((Office)dao) );		
		return JavalinJson.toJson(fmtv1);
	}

	@Override
	public String format(List<? extends CwmsDao> daoList) {
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
