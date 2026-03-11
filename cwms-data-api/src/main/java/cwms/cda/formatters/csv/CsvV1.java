package cwms.cda.formatters.csv;

import java.io.InputStream;
import java.time.ZoneId;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.csv.CwmsCsvDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.json.adapters.ZoneIdDeserializer;

public class CsvV1 implements CsvFormatter {

    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        String retVal;
        try {
            if (dto instanceof Office ) {
                retVal = new CsvV1Office().format(dto);
            } else if (dto instanceof LocationGroup ) {
                retVal =  new CsvV1LocationGroup().format(dto);
            } else if(dto instanceof CwmsCsvDTOBase) {
                CwmsCsvDTOBase csvDto = (CwmsCsvDTOBase) dto;
                CsvMapper csvMapper = buildObjectMapperCsvMetadataExcluded();
                CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
                String body = csvMapper.writer(headerSchema).writeValueAsString(dto);
                String comments = csvDto.buildMetadataComments();
                retVal = comments + body;
            } else {
                throw new FormattingException(dto.getClass().getName() + " is not currently supported for CSV formatting.");
            }
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not serialize:" + dto.getClass().getName(), e);
        }
        return retVal;
    }

    @Override
    public String formatWithMetaDataIncludedAsColumns(CwmsCsvDTOBase dto) {
        try {
            CsvMapper csvMapper = buildObjectMapperWithMetadataIncluded();
            CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
            return csvMapper.writer(headerSchema).writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not serialize:" + dto.getClass().getName(), e);
        }
    }


    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        String retVal = null;
        if (dtoList != null && !dtoList.isEmpty()) {
            CwmsDTOBase dto = dtoList.get(0);
            if (dto instanceof Office) {
                retVal = new CsvV1Office().format(dtoList);
            } else if(dto instanceof LocationGroup) {
                retVal = new CsvV1LocationGroup().format(dtoList);
            } else {
                throw new FormattingException("List of " + dto.getClass().getName() + " is not currently supported for CSV formatting.");
            }
        }
        return retVal;
    }

    @Override
    public String formatWithMetaDataIncludedAsComments(CwmsCsvDTOBase dto) {
        try {
            CsvMapper csvMapper = buildObjectMapperCsvMetadataExcluded();
            CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
            String body = csvMapper.writer(headerSchema).writeValueAsString(dto);
            String comments = dto.buildMetadataComments();
            return comments + body;
        }  catch (JsonProcessingException e) {
            throw new FormattingException("Could not serialize:" + dto.getClass().getName(), e);
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        T retVal = null;
        if (type.isAssignableFrom(Office.class)) {
            retVal = new CsvV1Office().parseContent(content, type);
        } else if (type.isAssignableFrom(LocationGroup.class)) {
            retVal = new CsvV1LocationGroup().parseContent(content, type);
        }
        return retVal;
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        T retVal = null;
        if (type.isAssignableFrom(Office.class)) {
            retVal = new CsvV1Office().parseContent(content, type);
        } else if (type.isAssignableFrom(LocationGroup.class)) {
            retVal = new CsvV1LocationGroup().parseContent(content, type);
        }
        return retVal;
    }
    
    private static CsvMapper buildObjectMapperWithMetadataIncluded() {
        CsvMapper csvMapper = buildObjectMapper();
        // Configure CSV to include @CsvMetadata columns by default
        AnnotationIntrospector defaults = csvMapper.getSerializationConfig().getAnnotationIntrospector();
        AnnotationIntrospector csvMetadata = new CsvMetadataIntrospector(true);
        AnnotationIntrospector ai = AnnotationIntrospectorPair.pair(csvMetadata, defaults);
        csvMapper.setAnnotationIntrospector(ai);
        return csvMapper;
    }
    
    private static CsvMapper buildObjectMapperCsvMetadataExcluded() {
        CsvMapper csvMapper = buildObjectMapper();
        AnnotationIntrospector defaults = csvMapper.getSerializationConfig().getAnnotationIntrospector();
        AnnotationIntrospector csvMetadata = new CsvMetadataIntrospector(false);
        AnnotationIntrospector ai = AnnotationIntrospectorPair.pair(csvMetadata, defaults);
        csvMapper.setAnnotationIntrospector(ai);
        return csvMapper;
    }

    private static CsvMapper buildObjectMapper() {
        CsvMapper retval = new CsvMapper();

        retval.findAndRegisterModules();
        // Without these two disables an Instant gets written as 3333333.335000000
        retval.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);

        // Do not force a global naming strategy here; allow DTO-level @JsonNaming
        // (e.g., JSON and XML strategies on the DTO) to dictate property names so CSV aligns.
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        retval.enable(CsvParser.Feature.ALLOW_COMMENTS);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ZoneId.class, new ZoneIdDeserializer());
        retval.registerModule(module);

        return retval;
    }

}
