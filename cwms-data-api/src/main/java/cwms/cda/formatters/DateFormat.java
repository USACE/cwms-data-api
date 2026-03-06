package cwms.cda.formatters;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public abstract class DateFormat {

    private DateFormat() {

    }

    public abstract void apply(ObjectMapper mapper, JavaTimeModule module);

    public static DateFormat epochMillis() {
        return new EpochMillis();
    }

    public static DateFormat pattern(String pattern) {
        return new PatternFormat(pattern);
    }

    public static final class EpochMillis extends DateFormat {
        @Override
        public void apply(ObjectMapper mapper, JavaTimeModule module) {
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof EpochMillis;
        }
    }

    public static final class PatternFormat extends DateFormat {
        private final String pattern;

        private PatternFormat(String pattern) {
            this.pattern = pattern;
        }

        @Override
        public void apply(ObjectMapper mapper, JavaTimeModule module) {
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            mapper.setDateFormat(new SimpleDateFormat(pattern));
            setCustomJavaTimeFormat(pattern, module);
        }

        private static void setCustomJavaTimeFormat(String dateFormatPattern, JavaTimeModule javaTimeModule) {
            DateTimeFormatter formatter =
                    DateTimeFormatter.ofPattern(dateFormatPattern).withZone(ZoneOffset.UTC);

            javaTimeModule.addSerializer(Instant.class, new JsonSerializer<>() {
                @Override
                public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers)
                        throws IOException {
                    if (value == null) {
                        gen.writeNull();
                    } else {
                        gen.writeString(formatter.format(value));
                    }
                }
            });

            javaTimeModule.addDeserializer(Instant.class, new JsonDeserializer<>() {
                @Override
                public Instant deserialize(JsonParser p, DeserializationContext ctxt)
                        throws IOException {
                    String text = p.getText();
                    if(text == null || text.isBlank())
                    {
                        return null;
                    }
                    return Instant.from(formatter.parse(text));
                }
            });

            javaTimeModule.addSerializer(ZonedDateTime.class, new JsonSerializer<>() {
                @Override
                public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider serializers)
                        throws IOException {
                    if (value == null) {
                        gen.writeNull();
                    } else {
                        gen.writeString(formatter.format(value.toInstant()));
                    }
                }
            });
            javaTimeModule.addDeserializer(ZonedDateTime.class, new JsonDeserializer<>() {
                @Override
                public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt)
                        throws IOException {
                    String text = p.getText();
                    if(text == null || text.isBlank())
                    {
                        return null;
                    }
                    return ZonedDateTime.ofInstant(
                            Instant.from(formatter.parse(text)),
                            ZoneOffset.UTC
                    );
                }
            });
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            PatternFormat that = (PatternFormat) o;
            return Objects.equals(pattern, that.pattern);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(pattern);
        }
    }

}
