package cwms.cda.formatters.csv;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.csv.CwmsCsvDTO;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class CsvExampleGenerator {

    private CsvExampleGenerator() {
        /* utility class */
    }

    /**
     * Retrieve example string for the given DTO Type.
     * @param dtoClass Cwms DTO type class
     * @return example data.
     */
    public static String getExample(Class<? extends CwmsCsvDTO<?>> dtoClass) {
        try {
            CwmsCsvDTO<?> dto = createDummyInstance(dtoClass);
            //show everything we can in the example, so include metadata and optional columns
            return new CsvV1().format(dto, new CsvConfiguration.Builder()
                    .withMetadataIncluded(true)
                    .withOptionalColumnsIncluded(true)
                    .build());
        } catch (Exception e) {
            //don't want to stop the world if we can't generate an example, but log the error for debugging
            FluentLogger.forEnclosingClass()
                        .atInfo()
                        .withCause(e)
                        .log("Failed to create csv example for " + dtoClass.getName() + ": ", e);
        }
        return "Could not generate example for " + dtoClass.getName();
    }

    /**
     * Create a temp instance of type T.
     * @param <T> CwmsCsvDTO  type to create
     * @param dtoClass Class&gt;T&lt; for dealing with type system.
     * @return Instance, may or may not be usable
     * @throws Exception any issues creating an instance
     */
    private static <T extends CwmsCsvDTO<?>> T createDummyInstance(Class<T> dtoClass) throws Exception {
        Class<?> builderClass = null;
        for (Class<?> inner : dtoClass.getDeclaredClasses()) {
            if (inner.getSimpleName().equals("Builder")) {
                builderClass = inner;
                break;
            }
        }

        if (builderClass == null) {
            Constructor<T> constr = dtoClass.getDeclaredConstructor();
            constr.setAccessible(true);
            return constr.newInstance();
        }

        Constructor<?> builderConstr = builderClass.getDeclaredConstructor();
        builderConstr.setAccessible(true);
        Object builder = builderConstr.newInstance();
        
        for (Field f : dtoClass.getDeclaredFields()) {
            Object val = getDummyValue(f);
            if (val != null) {
                String methodName = "with" + f.getName().substring(0, 1).toUpperCase() + f.getName().substring(1);
                try {
                    Method m = builderClass.getMethod(methodName, f.getType());
                    m.setAccessible(true);
                    m.invoke(builder, val);
                } catch (NoSuchMethodException ignored) {
                    try {
                        Method m2 = builderClass.getMethod(f.getName(), f.getType());
                        m2.setAccessible(true);
                        m2.invoke(builder, val);
                    } catch (NoSuchMethodException ignored2) {
                        // intentionally ignored
                    }
                }
            }
        }

        Method buildMethod = builderClass.getMethod("build");
        buildMethod.setAccessible(true);
        return dtoClass.cast(buildMethod.invoke(builder));
    }

    private static Object getDummyValue(Field f) throws Exception {
        Class<?> type = f.getType();
        if (f.isAnnotationPresent(CsvRows.class) && List.class.isAssignableFrom(type)) {
            ParameterizedType pt = (ParameterizedType) f.getGenericType();
            Class<?> rowType = (Class<?>) pt.getActualTypeArguments()[0];
            List<Object> rows = new ArrayList<>();
            rows.add(createDummyRow(rowType));
            return rows;
        }

        return getDummyValueSimple(type);
    }

    private static Object createDummyRow(Class<?> rowType) throws Exception {
        Class<?> builderClass = null;
        for (Class<?> inner : rowType.getDeclaredClasses()) {
            if (inner.getSimpleName().equals("Builder")) {
                builderClass = inner;
                break;
            }
        }

        if (builderClass == null) {
            try {
                Constructor<?> constr = rowType.getDeclaredConstructor();
                constr.setAccessible(true);
                return constr.newInstance();
            } catch (NoSuchMethodException e) {
                // If no default constructor and no builder, we might be in trouble for a generic generator.
                // But most DTOs should have one or the other.
                throw e;
            }
        }

        Constructor<?> builderConstr = builderClass.getDeclaredConstructor();
        builderConstr.setAccessible(true);
        Object builder = builderConstr.newInstance();
        for (Field f : rowType.getDeclaredFields()) {
            Object val = getDummyValueSimple(f.getType());
            if (val != null) {
                String methodName = "with" + f.getName().substring(0, 1).toUpperCase() + f.getName().substring(1);
                try {
                    Method m = builderClass.getMethod(methodName, f.getType());
                    m.setAccessible(true);
                    m.invoke(builder, val);
                } catch (NoSuchMethodException ignored) {
                    // Try without "with" prefix as some builders use that
                    try {
                        Method m2 = builderClass.getMethod(f.getName(), f.getType());
                        m2.setAccessible(true);
                        m2.invoke(builder, val);
                    } catch (NoSuchMethodException ignored2) {
                        // intentionally ignored
                    }
                }
            }
        }
        Method buildMethod = builderClass.getMethod("build");
        buildMethod.setAccessible(true);
        return buildMethod.invoke(builder);
    }

    @SuppressWarnings({"checkstyle:needbraces"})
    private static Object getDummyValueSimple(Class<?> type) {
        if (type == String.class) return "string";
        if (type == Instant.class) return Instant.now();
        if (type == Integer.class || type == int.class) return 0;
        if (type == Double.class || type == double.class) return 0.0;
        if (type == Long.class || type == long.class) return 0L;
        if (type == Boolean.class || type == boolean.class) return false;
        return null;
    }
}
