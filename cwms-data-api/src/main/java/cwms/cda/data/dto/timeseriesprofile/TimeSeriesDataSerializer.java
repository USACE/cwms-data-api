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

package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TimeSeriesDataSerializer extends StdSerializer<Map<Long, List<TimeSeriesData>>> {

    public TimeSeriesDataSerializer() {
        this(null);
    }

    public TimeSeriesDataSerializer(Class<Map<Long, List<TimeSeriesData>>> t) {
        super(t);
    }

    @Override
    public void serialize(Map<Long, List<TimeSeriesData>> timeSeriesData, JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        for (Map.Entry<Long, List<TimeSeriesData>> entry : timeSeriesData.entrySet()) {
            jsonGenerator.writeFieldName(entry.getKey().toString());
            jsonGenerator.writeStartArray();
            for (TimeSeriesData data : entry.getValue()) {
                jsonGenerator.writeStartArray();
                if (data != null) {
                    jsonGenerator.writeNumber(data.getValue());
                    jsonGenerator.writeNumber(data.getQuality());
                }
                jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndArray();
        }
        jsonGenerator.writeEndObject();
    }
}
