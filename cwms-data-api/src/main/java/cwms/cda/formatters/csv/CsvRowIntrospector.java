package cwms.cda.formatters.csv;

import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Jackson AnnotationIntrospector that enables @CsvRow to control CSV participation and column index.
 */
public class CsvRowIntrospector extends JacksonAnnotationIntrospector {

    private final ConcurrentMap<Class<?>, Boolean> classHasCsvRow = new ConcurrentHashMap<>();

    @Override
    public Integer findPropertyIndex(Annotated ann) {
        CsvRow csv = _findAnnotation(ann, CsvRow.class);
        if (csv != null && csv.index() >= 0) {
            return csv.index();
        }
        return super.findPropertyIndex(ann);
    }

    @Override
    public boolean hasIgnoreMarker(AnnotatedMember m) {
        Class<?> declaring = m.getDeclaringClass();
        boolean anyMarked = classHasCsvRow.computeIfAbsent(declaring, this::scanClassForCsvRow);
        if (anyMarked) {
            CsvRow onMember = _findAnnotation(m, CsvRow.class);
            if (onMember == null) {
                // If some members are marked with @CsvRow, ignore all that are not.
                return true;
            }
        }
        return super.hasIgnoreMarker(m);
    }

    private boolean scanClassForCsvRow(Class<?> cls) {
        // Check fields
        for (Field f : cls.getDeclaredFields()) {
            if (f.isAnnotationPresent(CsvRow.class)) {
                return true;
            }
        }
        // Check getters/setters or any methods
        for (Method m : cls.getDeclaredMethods()) {
            if (m.isAnnotationPresent(CsvRow.class)) {
                return true;
            }
        }
        return false;
    }
}
