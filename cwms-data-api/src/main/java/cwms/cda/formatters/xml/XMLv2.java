package cwms.cda.formatters.xml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.TimeSeries;
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
import java.util.List;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class XMLv2 implements OutputFormatter {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final XmlMapper XML_MAPPER = buildXmlMapper();
    private final XmlMapper om;

    public XMLv2() {
        this.om = XML_MAPPER;
    }

    public XMLv2(XmlMapper om) {
        this.om = om;
    }

    @Override
    public String getContentType() {
        return Formats.XMLV2;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            return om.writeValueAsString(dto);
        } catch (JsonProcessingException ex) {
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
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try {
            return om.writeValueAsString(dtoList);
        } catch (JsonProcessingException ex) {
            String msg = dtoList != null
                    ?
                    "Error rendering '" + dtoList + "' to XML"
                    :
                    "Null element passed to formatter";
            logger.atWarning().withCause(ex).log(msg);
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
     * Default instance of XmlMapper with suitable configuration of XML Version 2 Data.
     * @return XmlMapper instance.
     */
    public static @NotNull XmlMapper buildXmlMapper() {
        XmlMapper retval = new XmlMapper();
        retval.findAndRegisterModules();
        // Without these two disables an Instant gets written as 3333333.335000000
        retval.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        retval.addMixIn(TimeSeries.class, TimeSeriesXmlMixin.class);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(ZoneId.class, new ZoneIdDeserializer());
        module.addDeserializer(Instant.class, new FlexibleInstantDeserializer());
        retval.registerModule(module);
        return retval;
    }
}
