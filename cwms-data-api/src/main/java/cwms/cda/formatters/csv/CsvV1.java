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
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.csv.CwmsCsvRow;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.json.adapters.ZoneIdDeserializer;

public class CsvV1 implements CsvFormatter {

    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    /**
     * Default formatting does not include metadata in either columns or comments.
    **/
    @Override
    public String format(CwmsDTOBase dto) {
        String retVal;
        try {
            if (dto instanceof Office ) {
                retVal = new CsvV1Office().format(dto);
            } else if (dto instanceof LocationGroup ) {
                retVal =  new CsvV1LocationGroup().format(dto);
            } else if(dto instanceof CwmsCsvRow) {
                CsvMapper csvMapper = buildObjectMapperCsvMetadataExcluded();
                CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
                retVal = csvMapper.writer(headerSchema).writeValueAsString(dto);
            } else {
                throw new FormattingException(dto.getClass().getName() + " is not currently supported for CSV formatting.");
            }
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not serialize:" + dto.getClass().getName(), e);
        }
        return retVal;
    }

    @Override
    public String formatWithMetaDataIncludedAsColumns(CwmsCsvRow dto) {
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
            } else if(dto instanceof CwmsCsvRow) {
                try {
                    CsvMapper csvMapper = buildObjectMapperCsvMetadataExcluded();
                    CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
                    retVal = csvMapper.writer(headerSchema).writeValueAsString(dtoList);
                } catch (JsonProcessingException e) {
                    throw new FormattingException("Could not serialize list of:" + dto.getClass().getName(), e);
                }
            } else {
                throw new FormattingException("List of " + dto.getClass().getName() + " is not currently supported for CSV formatting.");
            }
        }
        return retVal;
    }

    @Override
    public String formatWithMetaDataIncludedAsComments(CwmsCsvRow dto) {
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
    public String formatWithMetaDataIncludedAsComments(List<? extends CwmsCsvRow> dtoList) {
        String retVal = null;
        if (dtoList != null && !dtoList.isEmpty()) {
            CwmsCsvRow dto = dtoList.get(0);
            try {
                CsvMapper csvMapper = buildObjectMapperCsvMetadataExcluded();
                CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
                String body = csvMapper.writer(headerSchema).writeValueAsString(dtoList);
                String comments = dto.buildMetadataComments();
                retVal = comments + body;
            } catch (JsonProcessingException e) {
                throw new FormattingException("Could not serialize list of:" + dto.getClass().getName(), e);
            }
        }
        return retVal;
    }

    @Override
    public String formatWithMetaDataIncludedAsColumns(List<? extends CwmsCsvRow> dtoList) {
        String retVal = null;
        if (dtoList != null && !dtoList.isEmpty()) {
            CwmsCsvRow dto = dtoList.get(0);
            try {
                CsvMapper csvMapper = buildObjectMapperWithMetadataIncluded();
                CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
                retVal = csvMapper.writer(headerSchema).writeValueAsString(dtoList);
            } catch (JsonProcessingException e) {
                throw new FormattingException("Could not serialize list of:" + dto.getClass().getName(), e);
            }
        }
        return retVal;
    }

    public static String stripHeaderWithMetadataComments(String csv) {
        StringBuilder sb = new StringBuilder();
        String[] lines = csv.split("\\r?\\n");
        boolean headerFound = false;
        for (String line : lines) {
            if (!headerFound) {
                if (!line.startsWith("#")) {
                    headerFound = true; // first non-comment line is the header
                }
                continue; //continue until we find the header, skipping all comment lines, and also skipping the header line itself
            }
            sb.append(line).append(System.lineSeparator());
        }
        return sb.toString();
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
        retval.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        retval.disable(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS);
        retval.enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING);

        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        retval.registerModule(new Jdk8Module());
        retval.enable(CsvParser.Feature.ALLOW_COMMENTS);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ZoneId.class, new ZoneIdDeserializer());
        retval.registerModule(module);

        return retval;
    }

}
