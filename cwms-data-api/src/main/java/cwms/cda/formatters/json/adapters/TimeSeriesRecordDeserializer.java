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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
	private static final String DATE_TIME = "date-time";
	private static final String VALUE = "value";
	private static final String QUALITY = "quality";

	@Override
	public TimeSeries.Record deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
		JsonNode node = jsonParser.readValueAsTree();
		if (node instanceof ObjectNode) {
			return parseObjectNode((ObjectNode) node);
		} else if (node instanceof ArrayNode) {
			return parseArrayNode((ArrayNode) node);
		} else {
			throw new IOException("Unexpected JSON node type: " + node.getNodeType());
		}
	}

	private TimeSeries.Record parseObjectNode(ObjectNode node) {
		Timestamp dateTime = node.get(DATE_TIME) == null ? null : new Timestamp(node.get(DATE_TIME).asLong());
		Double value = node.get(VALUE) == null || node.get(VALUE).asText().equalsIgnoreCase("null")
				? null : node.get(VALUE).asDouble();
		int quality = node.get(QUALITY) == null ? 0 : node.get(QUALITY).asInt();
		if (node.size() == 4) {
			Timestamp entryDate = new Timestamp(node.get(DATA_ENTRY_DATE).asLong());
			return new TimeSeriesRecordWithEntryDate(dateTime, value, quality, entryDate);
		} else {
			return new TimeSeries.StandardRecord(dateTime, value, quality);
		}
	}

	private TimeSeries.Record parseArrayNode(ArrayNode aNode) {
		Timestamp dateTime = aNode.get(0) == null ? null : new Timestamp(aNode.get(0).asLong());
		Double value = aNode.get(1) == null || aNode.get(1).asText().equalsIgnoreCase("null")
				? null : aNode.get(1).asDouble();
		int quality = aNode.get(2) == null ? 0 : aNode.get(2).asInt();
		if (aNode.size() == 4) {
			Timestamp entryDate = new Timestamp(aNode.get(3).asLong());
			return new TimeSeriesRecordWithEntryDate(dateTime, value, quality, entryDate);
		} else {
			return new TimeSeries.StandardRecord(dateTime, value, quality);
		}
	}
}