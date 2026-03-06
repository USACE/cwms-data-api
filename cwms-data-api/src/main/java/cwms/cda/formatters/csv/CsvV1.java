package cwms.cda.formatters.csv;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import cwms.cda.formatters.json.adapters.ZoneIdDeserializer;
import org.jetbrains.annotations.NotNull;

public class CsvV1 implements OutputFormatter {

    @Override
    public String getContentType() {
        return Formats.CSV;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        String retVal = null;
        try {
            if (dto instanceof Office ) {
                retVal = new CsvV1Office().format(dto);
            } else if (dto instanceof LocationGroup ) {
                retVal =  new CsvV1LocationGroup().format(dto);
            } else {
                AccessibleObject member = findCsvCollectionMember(dto.getClass());
                if (member != null) {
                    List<?> rows = extractCollection(dto, member);
                    if (rows == null || rows.isEmpty()) {
                        return ""; // nothing to serialize
                    }
                    Class<?> elementType = inferElementType(dto.getClass(), member, rows);
                    CsvMapper csvMapper = buildObjectMapper();
                    CsvSchema headerSchema = csvMapper.schemaFor(elementType).withHeader();
                    retVal = csvMapper.writer(headerSchema).writeValueAsString(rows);
                } else {
                    CsvMapper csvMapper = buildObjectMapper();
                    CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
                    retVal = csvMapper.writer(headerSchema).writeValueAsString(dto);
                }
            }
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not serialize:" + dto.getClass().getName(), e);
        }
        return retVal;
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        String retVal = null;
        if (dtoList != null && !dtoList.isEmpty()) {
            CwmsDTOBase dto = dtoList.get(0);
            try {
                if (dto instanceof Office) {
                    retVal = new CsvV1Office().format(dtoList);
                } else if(dto instanceof LocationGroup) {
                    retVal = new CsvV1LocationGroup().format(dtoList);
                } else {
                    AccessibleObject member = findCsvCollectionMember(dto.getClass());
                    if (member != null) {
                        // Flatten rows across all DTOs
                        List<Object> allRows = new ArrayList<>();
                        Class<?> commonElementType = null;
                        for (CwmsDTOBase item : dtoList) {
                            List<?> rows = extractCollection(item, member);
                            if (rows != null && !rows.isEmpty()) {
                                if (commonElementType == null) {
                                    commonElementType = inferElementType(item.getClass(), member, rows);
                                } else {
                                    Class<?> et = inferElementType(item.getClass(), member, rows);
                                    if (!commonElementType.equals(et)) {
                                        throw new FormattingException("Mismatched CSV row element types across collection-annotated DTOs: "
                                                + commonElementType.getName() + " vs " + et.getName());
                                    }
                                }
                                allRows.addAll(rows);
                            }
                        }
                        if (commonElementType == null) {
                            return ""; // no rows
                        }
                        CsvMapper csvMapper = buildObjectMapper();
                        CsvSchema headerSchema = csvMapper.schemaFor(commonElementType).withHeader();
                        retVal = csvMapper.writer(headerSchema).writeValueAsString(allRows);
                    } else {
                        CsvMapper csvMapper = buildObjectMapper();
                        CsvSchema headerSchema = csvMapper.schemaFor(dto.getClass()).withHeader();
                        retVal = csvMapper.writer(headerSchema).writeValueAsString(dtoList);
                    }
                }
            } catch (JsonProcessingException e) {
                throw new FormattingException("Could not serialize list of:" + dto.getClass().getName(), e);
            }
        }
        return retVal;
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        T retVal;
        try {
            if (type.isAssignableFrom(Office.class)) {
                retVal = new CsvV1Office().parseContent(content, type);
            } else if (type.isAssignableFrom(LocationGroup.class)) {
                retVal = new CsvV1LocationGroup().parseContent(content, type);
            } else {
                CsvMapper csvMapper = buildObjectMapper();
                CsvSchema withHeader = csvMapper.schemaFor(type).withHeader();
                try {
                    retVal = csvMapper.readerForListOf(type).with(withHeader).readValue(content);
                } catch (MismatchedInputException | IllegalArgumentException ex) {
                    CsvSchema noHeader = csvMapper.schemaFor(type).withoutHeader();
                    retVal = csvMapper.readerFor(type).with(noHeader).readValue(content);
                }
            }
        } catch (IOException e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
        return retVal;
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        try {
            if (type.isAssignableFrom(Office.class)) {
                return new CsvV1Office().parseContent(content, type);
            } else if (type.isAssignableFrom(LocationGroup.class)) {
                return new CsvV1LocationGroup().parseContent(content, type);
            } else {
                String text = new String(content.readAllBytes(), StandardCharsets.UTF_8);
                return parseContent(text, type);
            }
        } catch (IOException e) {
            throw new FormattingException("Could not deserialize: input stream", e);
        }
    }

    public static @NotNull CsvMapper buildObjectMapper() {
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

        // Configure CSV to honor @CsvRow for inclusion and optional index ordering
        AnnotationIntrospector defaults = retval.getSerializationConfig().getAnnotationIntrospector();
        AnnotationIntrospector csvRow = new CsvRowIntrospector();
        AnnotationIntrospector ai = AnnotationIntrospectorPair.pair(csvRow, defaults);
        retval.setAnnotationIntrospector(ai);

        return retval;
    }

    private AccessibleObject findCsvCollectionMember(Class<?> type) {
        AccessibleObject found = null;
        Class<?> cls = type;
        while (cls != null && cls != Object.class) {
            for (Field f : cls.getDeclaredFields()) {
                if (f.isAnnotationPresent(CsvCollectionRows.class)) {
                    if (found != null) {
                        throw new FormattingException("Multiple @CsvCollectionRows found in class hierarchy for " + type.getName());
                    }
                    f.setAccessible(true);
                    found = f;
                }
            }
            for (Method m : cls.getDeclaredMethods()) {
                if (m.isAnnotationPresent(CsvCollectionRows.class)) {
                    if (found != null) {
                        throw new FormattingException("Multiple @CsvCollectionRows found in class hierarchy for " + type.getName());
                    }
                    if (m.getParameterCount() == 0) {
                        m.setAccessible(true);
                        found = m;
                    }
                }
            }
            cls = cls.getSuperclass();
        }
        return found;
    }

    @SuppressWarnings("unchecked")
    private List<?> extractCollection(Object instance, AccessibleObject member) {
        try {
            Object val;
            if (member instanceof Field) {
                val = ((Field) member).get(instance);
            } else if (member instanceof Method) {
                val = ((Method) member).invoke(instance);
            } else {
                return null;
            }
            if (val == null) {
                return null;
            }
            if (val instanceof List) {
                return (List<?>) val;
            }
            if (val instanceof Collection) {
                return new ArrayList<>((Collection<?>) val);
            }
            throw new FormattingException("@CsvCollectionRows member is not a Collection on " + instance.getClass().getName());
        } catch (ReflectiveOperationException e) {
            throw new FormattingException("Unable to access @CsvCollectionRows member on " + instance.getClass().getName(), e);
        }
    }

    private Class<?> inferElementType(Class<?> ownerClass, AccessibleObject member, List<?> rows) {
        // Try generic signature first
        try {
            Type t = null;
            if (member instanceof Field) {
                t = ((Field) member).getGenericType();
            } else if (member instanceof Method) {
                t = ((Method) member).getGenericReturnType();
            }
            if (t instanceof ParameterizedType) {
                Type[] args = ((ParameterizedType) t).getActualTypeArguments();
                if (args.length == 1 && args[0] instanceof Class) {
                    return (Class<?>) args[0];
                }
            }
        } catch (Exception ignore) {
            // fall back to instance inspection
        }
        // Fallback: infer from first element
        Object first = rows.get(0);
        if (first != null) {
            return first.getClass();
        }
        // No reliable type information
        throw new FormattingException("Cannot infer element type for @CsvCollectionRows on " + ownerClass.getName());
    }
}
