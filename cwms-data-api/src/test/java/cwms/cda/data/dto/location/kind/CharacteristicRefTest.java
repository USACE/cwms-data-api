/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CharacteristicRefTest
{
	private static final String SPK = "SPK";
	private static final String CHARACTERISTIC = "Ogee Weir Depth";

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
	{
		CharacteristicRef ref = new CharacteristicRef.Builder()
				.withCharacteristicId(CHARACTERISTIC)
				.withOfficeId(SPK)
				.build();
		String json = Formats.format(test._contentType, ref);
		CharacteristicRef parsedRef = Formats.parseContent(test._contentType, json, CharacteristicRef.class);
		assertEquals(ref, parsedRef);
	}

	enum SerializationType
	{
		JSONV2(Formats.JSONV2),
		XMLV2(Formats.XMLV2),
		;

		final ContentType _contentType;

		SerializationType(String contentType)
		{
			_contentType = new ContentType(contentType);
		}
	}
}