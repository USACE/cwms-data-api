package cwms.cda.formatters.xml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import cwms.cda.formatters.json.adapters.FlexibleInstantDeserializer;
import cwms.cda.formatters.json.adapters.ZoneIdDeserializer;
import io.javalin.http.InternalServerErrorResponse;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class XMLv1 implements OutputFormatter {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final XmlMapper XML_MAPPER = buildObjectMapper();
    private final XmlMapper om;

    public XMLv1() {
        // default to using the shared static instance.
        this.om = XML_MAPPER;
    }

    public XMLv1(XmlMapper om) {
        this.om = om;
    }

    @Override
    public String getContentType() {
        return Formats.XML;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            if (dto instanceof Office) {
                return om.writeValueAsString(new XMLv1Office(Collections.singletonList((Office)dto)));
            }
            return om.writeValueAsString(dto);
        } catch (IOException ex) {
            String msg = dto != null
                    ?
                    "Error rendering '" + dto + "' to XML"
                    :
                    "Null element passed to formatter";
            logger.atWarning().withCause(ex).log(msg);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
    }

    @Override
    @SuppressWarnings("unchecked") // we're ALWAYS checking before conversion in this function
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try {
            if (!dtoList.isEmpty() && dtoList.get(0) instanceof Office) {
                return om.writeValueAsString(new XMLv1Office((List<Office>) dtoList));
            }
            return om.writeValueAsString(dtoList);
        } catch (Exception err) {
            logger.atWarning().withCause(err).log("Error doing XML format of office list");
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        try {
            return om.readValue(content, type);
        } catch (IOException e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        try {
            return om.readValue(content, type);
        } catch (IOException e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }

    /**
     * Create instance of XmlMapper with default settings for XML Version 1 Data.
     * @return XmlMapper instance
     */
    public static @NotNull XmlMapper buildObjectMapper() {
        XmlMapper retval = new XmlMapper();

        Set<Object> preModules = retval.getRegisteredModuleIds();

        // findAndRegisterModules searches the classpath and service-loads modules that it finds.
        // This isn't the most performant and it also has the downside that if you call
        // buildObjectMapper() from inside ForkJoinPool you may not get the Thread Context ClassLoader
        // and so the service loader may find a different version of Module class and throw exceptions
        // "not a subtype" when it tries to register it.
        retval.findAndRegisterModules();

        // The purpose of determining the modules that get automatically added is to Log them
        // and create a list of modules that should be manually registered.
        // once all the modules are being manually registered we can
        // remove the findAndRegisterModules call.
        Set<Object> postModules = retval.getRegisteredModuleIds();
        Set<Object> newModules = postModules.stream()
                .filter(module -> !preModules.contains(module))
                .collect(java.util.stream.Collectors.toSet());
        logger.atFine().log("These Modules got added by findAndRegisterModules: %s", newModules);

        // Without these two disables an Instant gets written as 3333333.335000000
        retval.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);

        retval.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        retval.addMixIn(VerticalDatumInfo.class, VerticalDatumInfoMixin.class);
        retval.addMixIn(VerticalDatumInfo.Builder.class, VerticalDatumInfoMixin.Builder.class);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(ZoneId.class, new ZoneIdDeserializer());
        module.addDeserializer(Instant.class, new FlexibleInstantDeserializer());
        retval.registerModule(module);
        return retval;
    }

}
