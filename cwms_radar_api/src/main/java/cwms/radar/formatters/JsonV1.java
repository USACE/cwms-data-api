package cwms.radar.formatters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.data.dto.Office;
import io.javalin.http.BadRequestResponse;
import io.javalin.plugin.json.JavalinJackson;
import service.annotations.FormatService;

@FormatService(contentType = Formats.JSON, dataTypes = {Office.class, Location.class})
public class JsonV1 implements OutputFormatter{

	private final ObjectMapper om;

	public JsonV1()
	{
		this(JavalinJackson.getObjectMapper());
	}

	public JsonV1(ObjectMapper om)
	{
		this.om = om.copy();
		this.om.setPropertyNamingStrategy(PropertyNamingStrategy.KEBAB_CASE);
		this.om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
	}

	@Override
	public String getContentType() {
		return Formats.JSON;
	}

	@Override
	public String format(CwmsDTO dto)
	{
		Object fmtv1 = buildFormatting(dto);
		try
		{
			return om.writeValueAsString(fmtv1);
		}
		catch(JsonProcessingException e)
		{
			throw new FormattingException("Could not format:" + dto, e);
		}
	}

	@Override
	public String format(List<? extends CwmsDTO> daoList)
	{
		Object wrapped = buildFormatting(daoList);
		try
		{
			return om.writeValueAsString(wrapped);
		}
		catch(JsonProcessingException e)
		{
			throw new FormattingException("Could not format list:" + daoList, e);
		}
	}

	private Object buildFormatting(CwmsDTO dao)
	{
		Object retval = null;

		if(dao instanceof Office)
		{
			List<Office> offices = Arrays.asList((Office) dao);
			retval = new OfficeFormatV1(offices);
		}
		else if(dao instanceof LocationGroup)
		{
			List<LocationGroup> groups = Arrays.asList((LocationGroup) dao);
			retval = new LocationGroupFormatV1(groups);
		}
		else if(dao instanceof LocationCategory)
		{
			List<LocationCategory> cats = Arrays.asList((LocationCategory) dao);
			retval = new LocationCategoryFormatV1(cats);
		}


		if(retval == null)
		{
			String klassName = "unknown";
			if(dao != null)
			{
				klassName = dao.getClass().getName();
			}
			throw new BadRequestResponse(
					String.format("Format %s not implemented for data of class:%s", getContentType(), klassName));
		}
		return retval;
	}
	
	private Object buildFormatting(List<? extends CwmsDTO> daoList)
	{
		Object retval = null;
		if(daoList != null && !daoList.isEmpty())
		{
			CwmsDTO firstObj = daoList.get(0);
			if(firstObj instanceof Office)
			{
				List<Office> officesList = daoList.stream().map(Office.class::cast).collect(Collectors.toList());
				retval = new OfficeFormatV1(officesList);
			}
			else if(firstObj instanceof LocationGroup)
			{
				List<LocationGroup> groups = daoList.stream().map(LocationGroup.class::cast).collect(Collectors.toList());
				retval = new LocationGroupFormatV1(groups);
			}
			else if(firstObj instanceof LocationCategory)
			{
				List<LocationCategory> cats = daoList.stream().map(LocationCategory.class::cast).collect(Collectors.toList());
				retval = new LocationCategoryFormatV1(cats);
			}


			if(retval == null)
			{
				String klassName = "unknown";
				if(firstObj != null)
				{
					klassName = firstObj.getClass().getName();
				}
				throw new BadRequestResponse(String.format("Format %s not implemented for data of class:%s", getContentType(), klassName));
			}
		}
		return retval;
	}
    
}
