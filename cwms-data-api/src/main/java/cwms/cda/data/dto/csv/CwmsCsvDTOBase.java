package cwms.cda.data.dto.csv;

import com.fasterxml.jackson.annotation.JsonProperty;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.csv.CsvMetadata;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class CwmsCsvDTOBase extends CwmsDTOBase {
    /**
     * Build a metadata comment block from fields/methods annotated with @CsvMetadata on the given DTO instance.
     * Format:
     * # metadata-count: N
     * # key: value
     * ...
     **/
    public String buildMetadataComments() {
        Class<?> cls = getClass();
        List<MemberMeta> metas = new ArrayList<>();

        // Collect fields
        for (Field f : cls.getDeclaredFields()) {
            if (f.getAnnotation(CsvMetadata.class) != null) {
                String key = resolveKeyName(f.getName(), f.getAnnotation(JsonProperty.class));
                Integer order = resolveIndex(f.getAnnotation(JsonProperty.class));
                Object value = null;
                try {
                    f.setAccessible(true);
                    value = f.get(this);
                } catch (Throwable ignore) { }
                metas.add(new MemberMeta(key, value, order));
            }
        }

        // Collect methods (e.g., getters) if annotated
        for (Method m : cls.getDeclaredMethods()) {
            if (m.getParameterCount() == 0 && m.getAnnotation(CsvMetadata.class) != null) {
                String base = m.getName();
                if (base.startsWith("get") && base.length() > 3) {
                    base = Character.toLowerCase(base.charAt(3)) + base.substring(4);
                }
                String key = resolveKeyName(base, m.getAnnotation(JsonProperty.class));
                Integer order = resolveIndex(m.getAnnotation(JsonProperty.class));
                Object value = null;
                try {
                    m.setAccessible(true);
                    value = m.invoke(this);
                } catch (Throwable ignore) { }
                metas.add(new MemberMeta(key, value, order));
            }
        }

        // Sort deterministically: by index if present, else by key name
        metas.sort((a, b) -> {
            if (a.index != null && b.index != null) {
                return Integer.compare(a.index, b.index);
            } else if (a.index != null) {
                return -1;
            } else if (b.index != null) {
                return 1;
            }
            return a.key.compareTo(b.key);
        });

        StringBuilder sb = new StringBuilder();
        sb.append("# metadata-count: ").append(metas.size()).append('\n');
        for (MemberMeta m : metas) {
            sb.append("# ").append(m.key).append(": ");
            if (m.value != null) {
                sb.append(m.value);
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static String resolveKeyName(String defaultName, JsonProperty jp) {
        if (jp != null && jp.value() != null && !jp.value().isEmpty()) {
            return jp.value();
        }
        return defaultName;
    }

    private static Integer resolveIndex(JsonProperty jp) {
        if (jp != null && jp.index() != JsonProperty.INDEX_UNKNOWN) {
            return jp.index();
        }
        return null;
    }

    private static class MemberMeta {
        final String key;
        final Object value;
        final Integer index;

        MemberMeta(String key, Object value, Integer index) {
            this.key = key;
            this.value = value;
            this.index = index;
        }
    }
}
