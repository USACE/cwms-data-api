package cwms.radar.formatters.csv;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import cwms.radar.data.dto.AssignedLocation;
import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.CwmsDTOBase;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OutputFormatter;
import io.swagger.v3.oas.annotations.media.Schema;


@Schema(
    name = "LocationGroup_CSV",
    description = "Single LocationGroup or List of LocationGroups in comma separated format",
    example =
    "#LocationGroup Id, OfficeId, Description, CategoryId, CategoryOfficeId, SharedLocAliasId, SharedRefLocationId, LocGroupAttribute\r\n"+
    "CERL,Construction Engineering Research Laboratory,Field Operating Activity	ERD\r\n"+
    "CHL,Coastal and Hydraulics Laboratory,Field Operating Activity	ERD\r\n" +
    "NAB,Baltimore District,District,NAD\r\n"+
    "NAD,North Atlantic Division,Division Headquarters,HQ"
)
public class CsvV1LocationGroup implements OutputFormatter{


    @Schema(hidden = true)
    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        LocationGroup locationGroup = (LocationGroup)dto;

        ObjectWriter writer = buildWriter();
        try
        {
            String s = writer.writeValueAsString(locationGroup);
            return "#LocationGroup " + s;
        }
        catch(JsonProcessingException e)
        {
            e.printStackTrace();
        }

        return null;
    }

    private ObjectWriter buildWriter()
    {
        CsvMapper mapper = new CsvMapper();
        mapper.addMixInAnnotations(LocationGroup.class, LocationGroupFormat.class);
        mapper.addMixInAnnotations(LocationCategory.class, LocationCategoryFormat.class);
        CsvSchema schema = mapper.schemaFor(LocationGroup.class)
                .withLineSeparator("\n")
                .withHeader();

        ObjectWriter writer = mapper.writer(schema);

        return writer;
    }

    @Override
    @SuppressWarnings("unchecked") // for the daoList conversion
    public String format(List<? extends CwmsDTOBase> dtoList) {
        List<LocationGroup> locationGroups = (List<LocationGroup>)dtoList;
        ObjectWriter writer = buildWriter();
        try
        {
            String s = writer.writeValueAsString(locationGroups);
            return  "#LocationGroup " + s;
        }
        catch(JsonProcessingException e)
        {
            e.printStackTrace();
        }


        return null;
    }


    // Mixin for LocationGroup
    // This class doesn't have to be related to LocationGroup, it just has to look like it.
    // We can add the annotations we want here and when LocationGroup is serialized it will
    // serialize like LocationGroupFormat
    @JsonPropertyOrder({"id",  "officeId", "description", "locationCategory",
            "sharedLocAliasId", "sharedRefLocationId", "locGroupAttribute" })
    private static abstract class LocationGroupFormat
    {

        @JsonProperty("Id")
        abstract String getId();

        @JsonProperty("OfficeId")
        abstract String getOfficeId();

        @JsonProperty("Description")
        abstract String getDescription();

        @JsonUnwrapped
        abstract LocationCategory getLocationCategory();

        @JsonProperty("SharedLocAliasId")
        abstract String getSharedLocAliasId();

        @JsonProperty("SharedRefLocationId")
        abstract String getSharedRefLocationId();

        @JsonProperty("LocGroupAttribute")
        abstract Number getLocGroupAttribute();

        @JsonIgnore
        abstract List<AssignedLocation> getAssignedLocations();

    }

    // Mixin for LocationCategory
    private static abstract class LocationCategoryFormat
    {
        @JsonProperty("CategoryOfficeId")
        abstract String getOfficeId();

        @JsonProperty("CategoryId")
        abstract String getId();

        @JsonIgnore
        abstract String getDescription();
    }
}
