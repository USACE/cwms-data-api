package cwms.radar.formatters.json;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.data.dto.Office;
import cwms.radar.data.dto.RecentValue;
import cwms.radar.data.dto.TimeSeriesCategory;
import cwms.radar.data.dto.TimeSeriesGroup;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.OfficeFormatV1;
import cwms.radar.formatters.OutputFormatter;
import io.javalin.http.BadRequestResponse;

import org.jetbrains.annotations.NotNull;
import service.annotations.FormatService;

@FormatService(contentType = Formats.JSON,
			   dataTypes = {
				   Office.class,
				   Location.class,
				   Catalog.class,
				   LocationGroup.class,
				   LocationCategory.class,
				   TimeSeriesCategory.class, TimeSeriesGroup.class,
				   Clob.class,
				   Clobs.class,
				   TimeSeriesGroup.class,
				   RecentValue.class
				})
public class JsonV1 implements OutputFormatter{

	private final ObjectMapper om;

	public JsonV1()
	{
		this(new ObjectMapper());
	}

	public JsonV1(ObjectMapper om)
	{
		this.om = om.copy();
		this.om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
		this.om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
	}

	@NotNull
	public static ObjectMapper buildObjectMapper()
	{
		return buildObjectMapper(new ObjectMapper());
	}

	@NotNull
	public static ObjectMapper buildObjectMapper(ObjectMapper om)
	{
		ObjectMapper retval = om.copy();

		retval.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
		retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		retval.registerModule(new JavaTimeModule());
		return retval;
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
		else if (dataTypesContains(dao.getClass())){
			// Any types that have to be handle as special cases
			// should be in else if branches before this
			// If the class is in the annotation assume we can just return it.
			retval = dao;
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

	private boolean dataTypesContains(Class klass){
		List<Class> dataTypes = getDataTypes();
		return dataTypes.contains(klass);
	}

	private List<Class> getDataTypes(){
		List<Class> retval = Collections.emptyList();
		FormatService fs = JsonV1.class.getDeclaredAnnotation(FormatService.class);
		if(fs != null)
		{
			Class[] classes = fs.dataTypes();
			if(classes != null && classes.length > 0)
			{
				retval = Arrays.asList(classes);
			}
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
			else if (dataTypesContains(firstObj.getClass())){
				// If dataType annotated with the class we can return an array of them.
				// If a class needs to be handled differently an else_if branch can be added above
				// here and a wrapper obj used to format the return value however is desired.
				retval = daoList;
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
