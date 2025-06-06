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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.TimeSeriesRecordWithEntryDate;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * A time-series record deserializer for properly deserializing JSON data.
 * Requires {@link cwms.cda.data.dto.TimeSeries.StandardRecord} class to avoid
 * a StackOverflowError when deserializing JSON data. This issue can be caused by the custom serializer
 * getting stuck in a loop if the Record class is used directly.
 * Allows for use of subclass with additional fields {@link TimeSeriesRecordWithEntryDate}.
 */
public final class TimeSeriesRecordDeserializer extends JsonDeserializer<TimeSeries.Record> {
	private static final String DATA_ENTRY_DATE = "data-entry-date";
	@Override
	public TimeSeries.Record deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
		JsonNode node = jsonParser.readValueAsTree();
		if (node.get(DATA_ENTRY_DATE) != null) {
			return jsonParser.getCodec().treeToValue(node, TimeSeriesRecordWithEntryDate.class);
		}
		String nodeString = node.toString();
		if (nodeString.startsWith("[")) {
			nodeString = nodeString.substring(1, nodeString.length() - 1);
			String[] valList = nodeString.split(",");
			if (valList.length == 4) {
				Timestamp dateTime = new Timestamp(Long.parseLong(valList[0]));
				Double value = valList[1] == null || valList[1].equalsIgnoreCase("null")
						? null : Double.parseDouble(valList[1]);
				int quality = Integer.parseInt(valList[2]);
				Timestamp entryDate = new Timestamp(Long.parseLong(valList[3]));
				return new TimeSeriesRecordWithEntryDate(dateTime, value, quality, entryDate);
			} else if (valList.length == 3) {
				Timestamp dateTime = new Timestamp(Long.parseLong(valList[0]));
				Double value = valList[1] == null || valList[1].equalsIgnoreCase("null")
						? null : Double.parseDouble(valList[1]);
				int quality = Integer.parseInt(valList[2]);
				return new TimeSeries.StandardRecord(dateTime, value, quality);
			} else {
				throw new IOException("Invalid TimeSeries Record format");
			}
		}
		return jsonParser.getCodec().treeToValue(node, TimeSeries.StandardRecord.class);
	}
}