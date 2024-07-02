/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.formatters.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.OutputFormatter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Formatter for CDA generated JSON.
 */
public class JsonV2 implements OutputFormatter {

    private final ObjectMapper om;

    public JsonV2() {
        this.om = buildObjectMapper();
    }

    @NotNull
    public static ObjectMapper buildObjectMapper() {
        ObjectMapper retVal = new ObjectMapper();

        retVal.findAndRegisterModules();
        // Without these two disables an Instant gets written as 3333333.335000000
        retVal.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        retVal.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);

        retVal.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        retVal.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        retVal.registerModule(new JavaTimeModule());
        return retVal;
    }

    @Override
    public String getContentType() {
        return Formats.JSONV2;
    }

    @Override
    public String format(CwmsDTOBase dto) {
        try {
            return om.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not format :" + dto, e);
        }
    }

    @Override
    public String format(List<? extends CwmsDTOBase> dtoList) {
        try {
            return om.writeValueAsString(dtoList);
        } catch (JsonProcessingException e) {
            throw new FormattingException("Could not format :" + dtoList, e);
        }
    }

    @Override
    public <T extends CwmsDTOBase> T parseContent(String content, Class<T> type) {
        try {
            return om.readValue(content, type);
        } catch (JsonProcessingException e) {
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

    @Override
    public <T extends CwmsDTOBase> List<T> parseContentList(String content, Class<T> type) {
        try {
            return om.readValue(content, om.getTypeFactory().constructCollectionType(List.class, type));
        } catch (IOException e) {
            throw new FormattingException("Could not deserialize:" + content, e);
        }
    }
}
