package cwms.cda.formatters.xml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import io.javalin.http.InternalServerErrorResponse;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XMLv2 implements OutputFormatter {
    private static final Logger logger = Logger.getLogger(XMLv2.class.getName());

    public XMLv2() {
    }

    @Override
    public String getContentType() {
        return Formats.XMLV2;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            return buildXmlMapper().writeValueAsString(dto);
        } catch (JsonProcessingException ex) {
            String msg = dto != null ?
                    "Error rendering '" + dto + "' to XML"
                    :
                    "Null element passed to formatter";
            logger.log(Level.WARNING, msg, ex);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try {
            return buildXmlMapper().writeValueAsString(dtoList);
        } catch (JsonProcessingException ex) {
            String msg = dtoList != null ?
                    "Error rendering '" + dtoList + "' to XML"
                    :
                    "Null element passed to formatter";
            logger.log(Level.WARNING, msg, ex);
            throw new InternalServerErrorResponse("Invalid Parameters");
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        try {
            XmlMapper retval = buildXmlMapper();
            return retval.readValue(content, type);
        } catch (IOException e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(InputStream content, Class<T> type) {
        try {
            XmlMapper retval = buildXmlMapper();
            return retval.readValue(content, type);
        } catch (IOException e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }

    private static @NotNull XmlMapper buildXmlMapper() {
        XmlMapper retval = new XmlMapper();
        retval.findAndRegisterModules();
        // Without these two disables an Instant gets written as 3333333.335000000
        retval.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retval.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        retval.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retval.registerModule(new JavaTimeModule());
        retval.addMixIn(TimeSeries.class, TimeSeriesXmlMixin.class);
        return retval;
    }
}
