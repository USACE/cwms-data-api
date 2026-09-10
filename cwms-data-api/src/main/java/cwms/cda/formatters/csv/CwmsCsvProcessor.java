package cwms.cda.formatters.csv;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.csv.CsvRequiredColumn;
import cwms.cda.data.dto.csv.CsvUnitHeader;
import cwms.cda.data.dto.csv.CwmsCsvDTO;
import cwms.cda.formatters.DateFormat;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.json.adapters.ZoneIdDeserializer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class to processing cwms CSV DTOs for both reading and writing.
 * Handles building header lines with units, building metadata comment lines,
 * and parsing metadata and units from CSV content.
 */
public final class CwmsCsvProcessor {

    private CwmsCsvProcessor() {
        /* utility class */
    }

    private static String buildMetadataComments(Object dto) {
        StringBuilder sb = new StringBuilder();
        Class<?> cls = dto.getClass();
        List<Field> fields = getAllFields(cls);
        for (Field f : fields) {
            if (f.isAnnotationPresent(CsvRows.class)) {
                continue;
            }
            f.setAccessible(true);
            try {
                Object val = f.get(dto);
                if (val != null) {
                    String key = resolveKeyName(f, cls);
                    sb.append("# ").append(key).append(": ").append(val).append("\n");
                }
            } catch (IllegalAccessException ex) {
                throw new FormattingException("Error building metadata comments for " + cls.getName(), ex);
            }
        }
        return sb.toString();
    }

    private static List<Field> getAllFields(Class<?> cls) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = cls;
        while (current != null && current != Object.class) {
            fields.addAll(Arrays.asList(current.getDeclaredFields()));
            current = current.getSuperclass();
        }
        return fields;
    }

    private static String buildHeader(Object dto, boolean includeOptionalColumns) {
        StringBuilder sb = new StringBuilder();
        Map<String, String> fieldToUnits = new HashMap<>();

        // Find row type and examples to get units from
        Class<?> rowType = null;
        Object firstRow = null;
        if (dto instanceof CwmsCsvDTO) {
            List<?> rows = ((CwmsCsvDTO<?>) dto).getRows();
            if (rows != null && !rows.isEmpty()) {
                firstRow = rows.get(0);
                rowType = firstRow.getClass();
            }
        } else if (dto instanceof List && !((List<?>) dto).isEmpty()) {
            firstRow = ((List<?>) dto).get(0);
            rowType = firstRow.getClass();
        } else {
            rowType = dto.getClass();
        }

        try {
            // Check top-level DTO first
            extractFieldToUnits(dto, fieldToUnits);

            // If it's a list or CwmsCsv, also check the row object for @CsvUnitHeader
            if (firstRow != null) {
                extractFieldToUnits(firstRow, fieldToUnits);
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new FormattingException("Error extracting units for CSV header", e);
        }

        if (rowType != null) {
            List<ColumnInfo> columns = new ArrayList<>();
            for (Field f : getAllFields(rowType)) {
                JsonProperty jp = f.getAnnotation(JsonProperty.class);
                if (jp != null && jp.index() != JsonProperty.INDEX_UNKNOWN) {
                    if (!includeOptionalColumns && !f.isAnnotationPresent(CsvRequiredColumn.class)) {
                        continue;
                    }
                    String name = resolvePropertyName(f);
                    String units = fieldToUnits.get(name);
                    if (units != null) {
                        name = name + " (" + units + ")";
                    }
                    columns.add(new ColumnInfo(name, jp.index()));
                }
            }
            columns.sort(Comparator.comparingInt(c -> c.order));
            for (int i = 0; i < columns.size(); i++) {
                sb.append(columns.get(i).name);
                if (i < columns.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }

    private static void extractFieldToUnits(Object dto, Map<String, String> fieldToUnits)
        throws IllegalAccessException, InvocationTargetException {
        if (dto == null) { 
            return;
        }
        Class<?> cls = dto.getClass();
        List<Field> fields = getAllFields(cls);
        for (Field f : fields) {
            CsvUnitHeader ann = f.getAnnotation(CsvUnitHeader.class);
            if (ann != null) {
                f.setAccessible(true);
                Object val = f.get(dto);
                if (val != null) {
                    String unitVal = val.toString();
                    if (!unitVal.isEmpty()) {
                        fieldToUnits.put(ann.field(), unitVal);
                    }
                }
            }
        }
        List<Method> methods = getAllMethods(cls);
        for (Method m : methods) {
            CsvUnitHeader ann = m.getAnnotation(CsvUnitHeader.class);
            if (ann != null && m.getParameterCount() == 0) {
                m.setAccessible(true);
                Object val = m.invoke(dto);
                if (val != null) {
                    String unitVal = val.toString();
                    if (!unitVal.isEmpty()) {
                        fieldToUnits.put(ann.field(), unitVal);
                    }
                }
            }
        }
    }

    static <T extends CwmsDTOBase> T parseCwmsCsv(String content, Class<T> type) {
        try {
            T dto;
            try {
                dto = type.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException e) {
                // No default constructor, try to find a Builder
                Class<?> builderClass = null;
                for (Class<?> inner : type.getDeclaredClasses()) {
                    if (inner.getSimpleName().equals("Builder")) {
                        builderClass = inner;
                        break;
                    }
                }
                if (builderClass != null) {
                    Object builder = builderClass.getDeclaredConstructor().newInstance();
                    Method buildMethod = builderClass.getDeclaredMethod("build");
                    dto = type.cast(buildMethod.invoke(builder));
                } else {
                    throw e;
                }
            }

            if (dto instanceof CwmsCsvDTO) {
                Map<String, String> metadata = CwmsCsvProcessor.parseMetadata(content);
                String units = CwmsCsvProcessor.parseUnits(content);
                CwmsCsvProcessor.injectMetadataAndUnits(content, type, dto, metadata, units);
                return dto;
            }
        } catch (Exception e) {
            throw new FormattingException("Could not parse " + type.getName(), e);
        }
        throw new FormattingException("Could not parse " + type.getName()
                                     + ". Must be a " + CwmsCsvDTO.class.getName());
    }

    private static <T extends CwmsDTOBase> void injectMetadataAndUnits(String content, Class<T> type,
                                                                       T dto, Map<String, String> metadata,
                                                                       String units)
        throws IOException, IllegalAccessException {
        // Inject metadata into DTO
        CwmsCsvProcessor.applyMetadataAndUnits(dto, metadata, units);

        Field rowsField = getRowsField(type);

        if (rowsField != null) {
            rowsField.setAccessible(true);
            Class<?> rowType = (Class<?>) ((ParameterizedType) rowsField.getGenericType()).getActualTypeArguments()[0];
            List<?> rows = parseRows(content, rowType);

            if (units != null) {
                for (Object row : rows) {
                    CwmsCsvProcessor.applyMetadataAndUnits(row, metadata, units);
                }
            }

            rowsField.set(dto, rows);
        }
    }

    private static List<?> parseRows(String content, Class<?> csvRowDtoType) throws IOException {
        CsvMapper csvMapper = buildObjectMapper(csvRowDtoType, new CsvConfiguration.Builder().build());
        csvMapper.enable(CsvParser.Feature.ALLOW_COMMENTS);
        csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        csvMapper.enable(CsvParser.Feature.TRIM_SPACES);
        CsvSchema schema = csvMapper.schemaFor(csvRowDtoType).withHeader();
        try (MappingIterator<?> it = csvMapper.readerFor(csvRowDtoType).with(schema).readValues(content)) {
            return it.readAll();
        }
    }

    static String formatCwmsCsv(CwmsCsvDTO<?> dto, CsvConfiguration config) throws JsonProcessingException {
        StringBuilder sb = new StringBuilder();

        if (config.includeMetadata()) {
            sb.append(buildMetadataComments(dto));
        }

        List<?> rows = dto.getRows();
        if (rows != null && !rows.isEmpty()) {
            Object firstRow = rows.get(0);

            CsvMapper csvMapper = buildObjectMapper(firstRow.getClass(), config);

            CsvSchema.Builder schemaBuilder = CsvSchema.builder();

            List<Field> fields = getAllFields(firstRow.getClass());

            fields.stream()
                    .filter(f -> {
                        JsonProperty jp = f.getAnnotation(JsonProperty.class);
                        if (jp == null || jp.index() == JsonProperty.INDEX_UNKNOWN) {
                            return false;
                        }
                        return config.includeOptionalColumns() || f.isAnnotationPresent(CsvRequiredColumn.class);
                    })
                    .sorted(Comparator.comparingInt(f -> f.getAnnotation(JsonProperty.class).index()))
                    .forEach(f -> schemaBuilder.addColumn(resolvePropertyName(f)));

            CsvSchema schema = schemaBuilder.build();
            String header = buildHeader(dto, config.includeOptionalColumns());
            sb.append(header);
            FilterProvider filters = new SimpleFilterProvider()
                    .addFilter("columnFilter",
                               SimpleBeanPropertyFilter.filterOutAllExcept(new HashSet<>(schema.getColumnNames())));
            sb.append(csvMapper.writer(schema).with(filters).writeValueAsString(rows));
        }
        return sb.toString();
    }

    private static CsvMapper buildObjectMapper(Class<?> rowType, CsvConfiguration config) {
        CsvMapper mapper = new CsvMapper();
        // Without these two disables an Instant gets written as 3333333.335000000
        mapper.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        mapper.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);
        mapper.disable(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS);
        mapper.enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        
        mapper.registerModule(new Jdk8Module());
        mapper.enable(CsvParser.Feature.ALLOW_COMMENTS);
        mapper.addMixIn(rowType, PropertyFilterMixIn.class);

        JavaTimeModule javaTimeModule = new JavaTimeModule();
        DateFormat dateFormat = config.getDateFormat();
        dateFormat.apply(mapper, javaTimeModule);
        mapper.registerModule(javaTimeModule);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(ZoneId.class, new ZoneIdDeserializer());
        mapper.registerModule(module);
        return mapper;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonFilter("columnFilter")
    abstract static class PropertyFilterMixIn {
    }

    private static <T extends CwmsDTOBase> @Nullable Field getRowsField(Class<T> type) {
        // Find rows field
        Field rowsField = null;
        for (Field f : CwmsCsvProcessor.getAllFields(type)) {
            if (f.isAnnotationPresent(CsvRows.class)) {
                rowsField = f;
                break;
            }
        }
        return rowsField;
    }

    private static List<Method> getAllMethods(Class<?> cls) {
        List<Method> methods = new ArrayList<>();
        Class<?> current = cls;
        while (current != null && current != Object.class) {
            methods.addAll(Arrays.asList(current.getDeclaredMethods()));
            current = current.getSuperclass();
        }
        return methods;
    }

    private static String resolvePropertyName(Field f) {
        JsonProperty jp = f.getAnnotation(JsonProperty.class);
        if (jp != null && !jp.value().isEmpty()) {
            return jp.value();
        }
        JsonNaming naming = f.getDeclaringClass().getAnnotation(JsonNaming.class);
        return getName(f, naming);
    }

    private static String getName(Field f, JsonNaming naming) {
        if (naming != null) {
            try {
                Object strat = naming.value().getDeclaredConstructor().newInstance();
                if (strat instanceof PropertyNamingStrategies.NamingBase) {
                    return ((PropertyNamingStrategies.NamingBase) strat).translate(f.getName());
                }
            } catch (InvocationTargetException | InstantiationException 
                    | IllegalAccessException | NoSuchMethodException e) {
                throw new FormattingException("Error resolving property name for " + f.getName(), e);
            }
        }
        return f.getName();
    }

    private static String resolveKeyName(Field f, Class<?> owner) {
        JsonProperty jp = f.getAnnotation(JsonProperty.class);
        if (jp != null && !jp.value().isEmpty()) {
            return jp.value();
        }
        JsonNaming naming = owner.getAnnotation(JsonNaming.class);
        return getName(f, naming);
    }

    @SuppressWarnings({"checkstyle:NeedBraces"}) // always returns, would get really noisy.
    private static Object convertToType(String val, Class<?> type) {
        if (type == String.class) return val;
        if (type == Instant.class) return Instant.parse(val);
        if (type == Integer.class || type == int.class) return Integer.parseInt(val);
        if (type == Double.class || type == double.class) return Double.parseDouble(val);
        if (type == Long.class || type == long.class) return Long.parseLong(val);
        if (type == Boolean.class || type == boolean.class) return Boolean.parseBoolean(val);
        return null;
    }

    private static void applyMetadataAndUnits(Object dto, Map<String, String> metadata, String units) {
        if (dto == null) {
            return;
        }
        Class<?> cls = dto.getClass();
        // Use a list of all fields including superclasses
        List<Field> allFields = CwmsCsvProcessor.getAllFields(cls);

        for (Field f : allFields) {
            f.setAccessible(true);
            try {
                // Handle Units via @CsvUnitHeader
                if (units != null) {
                    CsvUnitHeader unitAnn = f.getAnnotation(CsvUnitHeader.class);
                    if (unitAnn != null) {
                        f.set(dto, units);
                    }
                }

                // Handle Metadata
                String key = CwmsCsvProcessor.resolveKeyName(f, cls);
                String val = metadata.get(key);
                if (val != null) {
                    Object converted = CwmsCsvProcessor.convertToType(val, f.getType());
                    if (converted != null) {
                        f.set(dto, converted);
                    }
                }
            } catch (IllegalAccessException e) {
                throw new FormattingException("Error applying metadata to field " + f.getName(), e);
            }
        }
    }

    private static Map<String, String> parseMetadata(String content) {
        Map<String, String> metadata = new HashMap<>();
        String[] lines = content.split("\\r?\\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("#")) {
                String comment = trimmed.substring(1).trim();
                int colon = comment.indexOf(':');
                if (colon != -1) {
                    String key = comment.substring(0, colon).trim();
                    String val = comment.substring(colon + 1).trim();
                    metadata.put(key, val);
                }
            } else if (!trimmed.isEmpty()) {
                break; // Header or data starts
            }
        }
        return metadata;
    }

    private static String parseUnits(String content) {
        String[] lines = content.split("\\r?\\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.startsWith("#")) {
                continue;
            }
            if (trimmed.isEmpty()) {
                continue;
            }
            // First non-comment line is header
            int start = trimmed.indexOf('(');
            int end = trimmed.indexOf(')');
            if (start != -1 && end != -1 && end > start) {
                return trimmed.substring(start + 1, end);
            }
            break;
        }
        return null;
    }

    private static class ColumnInfo {
        final String name;
        final int order;

        ColumnInfo(String name, int order) {
            this.name = name;
            this.order = order;
        }
    }
}
