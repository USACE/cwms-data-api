package cwms.cda.formatters;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface ObjectMapperFormatter extends OutputFormatter {
    ObjectMapper getObjectMapper();
}
