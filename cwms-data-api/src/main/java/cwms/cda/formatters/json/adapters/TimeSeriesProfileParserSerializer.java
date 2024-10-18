/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.formatters.json.adapters;

import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserBase.COLUMNAR_TYPE;
import static cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserBase.INDEXED_TYPE;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoColumnar;
import cwms.cda.data.dto.timeseriesprofile.ParameterInfoIndexed;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserColumnar;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
import java.io.IOException;


public class TimeSeriesProfileParserSerializer extends JsonSerializer<TimeSeriesProfileParser> {
    @Override
    public void serialize(TimeSeriesProfileParser value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
        String type = value.getClass().getAnnotation(JsonTypeName.class).value();
        gen.writeStartObject();
        gen.writeObjectField("type", type);
        gen.writeObjectField("location-id", value.getLocationId());
        gen.writeObjectField("key-parameter", value.getKeyParameter());
        gen.writeObjectField("record-delimiter", value.getRecordDelimiter());
        gen.writeObjectField("time-format", value.getTimeFormat());
        gen.writeObjectField("time-zone", value.getTimeZone());
        gen.writeFieldName("parameter-info-list");
        gen.writeStartArray();
        for (int i = 0; i < value.getParameterInfoList().size(); i++) {
            gen.writeStartObject();
            String parameterType = value.getParameterInfoList().get(i).getClass()
                    .getAnnotation(JsonTypeName.class).value();
            gen.writeObjectField("type", parameterType);
            if (parameterType.equalsIgnoreCase("indexed-parameter-info")) {
                gen.writeObjectField("index",
                        ((ParameterInfoIndexed) value.getParameterInfoList().get(i)).getIndex());
            } else if (parameterType.equalsIgnoreCase("columnar-parameter-info")) {
                gen.writeObjectField("start-column",
                        ((ParameterInfoColumnar) value.getParameterInfoList().get(i)).getStartColumn());
                gen.writeObjectField("end-column",
                        ((ParameterInfoColumnar)value.getParameterInfoList().get(i)).getEndColumn());
            }
            gen.writeObjectField("parameter", value.getParameterInfoList().get(i).getParameter());
            gen.writeObjectField("unit", value.getParameterInfoList().get(i).getUnit());
            gen.writeEndObject();
        }
        gen.writeEndArray();
        gen.writeObjectField("time-in-two-fields", value.getTimeInTwoFields());
        if (type.equalsIgnoreCase(INDEXED_TYPE)) {
            gen.writeObjectField("field-delimiter",
                    ((TimeSeriesProfileParserIndexed) value).getFieldDelimiter());
            gen.writeObjectField("time-field",
                    ((TimeSeriesProfileParserIndexed) value).getTimeField());
        } else if (type.equalsIgnoreCase(COLUMNAR_TYPE)) {
            gen.writeObjectField("time-start-column",
                    ((TimeSeriesProfileParserColumnar) value).getTimeStartColumn());
            gen.writeObjectField("time-end-column",
                    ((TimeSeriesProfileParserColumnar) value).getTimeEndColumn());
        }
        gen.writeEndObject();
    }

    @Override
    public void serializeWithType(TimeSeriesProfileParser value, JsonGenerator gen, SerializerProvider serializers,
            TypeSerializer typeSer) throws IOException {
        serialize(value, gen, serializers); // call your customized serialize method
    }
}
