package cwms.radar.formatters.json;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.Clob;
import cwms.radar.data.dto.Clobs;
import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.Office;
import cwms.radar.data.dto.Pool;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.OutputFormatter;
import io.javalin.plugin.json.JavalinJackson;
import service.annotations.FormatService;

@FormatService(contentType = Formats.JSONV2, dataTypes = {
	Office.class,
	Location.class,
	Catalog.class,
	TimeSeries.class,
	Clob.class,
	Clobs.class,
	Pool.class
})
public class JsonV2 implements OutputFormatter {

	private final ObjectMapper om;

	public JsonV2()
	{
		this(JavalinJackson.getObjectMapper());
	}

	public JsonV2(ObjectMapper om)
	{
		this.om = om.copy();
		this.om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
		this.om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		this.om.registerModule(new JavaTimeModule());
	}

	@Override
	public String getContentType() {
		return Formats.JSONV2;
	}

	@Override
	public String format(CwmsDTO dto) {
		try
		{
			return om.writeValueAsString(dto);
		}
		catch(JsonProcessingException e)
		{
			throw new FormattingException("Could not format :" + dto, e);
		}
	}

	@Override
	public String format(List<? extends CwmsDTO> dtoList) {
		try
		{
			return om.writeValueAsString(dtoList);
		}
		catch(JsonProcessingException e)
		{
			throw new FormattingException("Could not format :" + dtoList, e);
		}
	}

}
