package cwms.radar.data.dto;

import java.util.ArrayList;

import cwms.radar.formatters.xml.XMLv1;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogTest
{
	@Test
	void test_xml_format(){
		Catalog catalog  = new Catalog(null,66, 5, new ArrayList<>());

		XMLv1 xmLv1 = new XMLv1();
		String xmlStr = xmLv1.format(catalog);
		assertNotNull(xmlStr);

		assertFalse(xmlStr.contains("pageSize"));
		assertTrue(xmlStr.contains("page-size"));
	}
}
