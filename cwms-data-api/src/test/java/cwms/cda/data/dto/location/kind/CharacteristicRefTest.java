package cwms.cda.data.dto.location.kind;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CharacteristicRefTest
{
	private static final String SPK = "SPK";
	private static final String CHARACTERISTIC = "Characteristic";

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