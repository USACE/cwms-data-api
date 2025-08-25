/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.dataformat.xml.ser.XmlSerializerProvider;
import cwms.cda.data.dto.TimeSeries;
import java.io.IOException;

public class TimeSeriesRecordSerializer extends StdSerializer<TimeSeries.Record> {
    // Default constructor for Jackson
    public TimeSeriesRecordSerializer() {
        super(TimeSeries.Record.class);
    }

    @Override
    public void serialize(TimeSeries.Record recordValue, JsonGenerator gen, SerializerProvider provider)
        throws IOException {

        if (provider instanceof XmlSerializerProvider) {
            // Handle XML serialization

            gen.writeStartObject();
            gen.writeNumberField("date-time", recordValue.getDateTime().getTime());
            if (recordValue.getValue() == null) {
                gen.writeNullField("value");
            } else {
                gen.writeNumberField("value", recordValue.getValue());
            }
            gen.writeNumberField("quality-code", recordValue.getQualityCode());
            gen.writeEndObject();
        } else {
            // Handle JSON serialization

            gen.writeStartArray();
            gen.writeNumber(recordValue.getDateTime().getTime());
            if (recordValue.getValue() == null) {
                gen.writeNull();
            } else {
                gen.writeNumber(recordValue.getValue());
            }
            gen.writeNumber(recordValue.getQualityCode());
            // Used to include the dataEntryDate in the serialized output if requested. Modifies length of the output array.
            // If the dataEntryDate is requested, it will always be non-null
            // Without the dataEntryDate, the array will have 3 elements: [dateTime, value, qualityCode]
            if (recordValue.getDataEntryDate() != null) {
                gen.writeNumber(recordValue.getDataEntryDate().getTime());
            }
            gen.writeEndArray();
        }
    }
}
