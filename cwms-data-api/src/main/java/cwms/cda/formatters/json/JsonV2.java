package cwms.cda.formatters.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.Clob;
import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.CwmsDTO;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LocationLevels;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.Pool;
import cwms.cda.data.dto.Pools;
import cwms.cda.data.dto.SpecifiedLevel;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptors;
import cwms.cda.data.dto.rating.ExpressionRating;
import cwms.cda.data.dto.rating.RatingMetadata;
import cwms.cda.data.dto.rating.RatingMetadataList;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.data.dto.rating.RatingSpecs;
import cwms.cda.data.dto.rating.RatingTemplate;
import cwms.cda.data.dto.rating.RatingTemplates;
import cwms.cda.data.dto.rating.TableRating;
import cwms.cda.data.dto.rating.TransitionalRating;
import cwms.cda.data.dto.rating.UsgsStreamRating;
import cwms.cda.data.dto.rating.VirtualRating;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import service.annotations.FormatService;

@FormatService(contentType = Formats.JSONV2, dataTypes = {
        Office.class,
        Location.class,
        Catalog.class,
        TimeSeries.class,
        Clob.class,
        Clobs.class,
        Pool.class,
        Pools.class,
        Blobs.class,
        Blobs.class,
        SpecifiedLevel.class,
        RatingTemplate.class, RatingTemplates.class,
        RatingMetadataList.class, RatingMetadata.class,
        TableRating.class, TransitionalRating.class, VirtualRating.class,
        ExpressionRating.class, UsgsStreamRating.class,
        RatingSpec.class, RatingSpecs.class,
        LocationLevel.class, LocationLevels.class,
        TimeSeriesIdentifierDescriptor.class, TimeSeriesIdentifierDescriptors.class
})
/**
 * Formatter for CDA generated JSON.
 */
public class JsonV2 implements OutputFormatter {

    private final ObjectMapper om;

    public JsonV2() {
        this(new ObjectMapper());
    }

    public JsonV2(ObjectMapper om) {
        this.om = buildObjectMapper(om);
    }

    @NotNull
    public static ObjectMapper buildObjectMapper() {
        return buildObjectMapper(new ObjectMapper());
    }

    @NotNull
    public static ObjectMapper buildObjectMapper(ObjectMapper om) {
        ObjectMapper retval = om.copy();

        retval.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        return retval;
    }

    @Override
    public String getContentType() {
        return Formats.JSONV2;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            return om.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not format :" + dto, e);
        }
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try {
            return om.writeValueAsString(dtoList);
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not format :" + dtoList, e);
        }
    }

}
