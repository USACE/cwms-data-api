package cwms.cda.data.dto;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.Catalog.CatalogPage;
import cwms.cda.formatters.xml.XMLv1;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

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


	@Test
	void test_catalog_page_round_trip() throws Exception {
		final CatalogPage page = new CatalogPage("SPK/a", 
											null,
											".*",
											null,
											null,
											null,
											null);
		final String pageString = Catalog.encodeCursor(page.toString(),10,100);
		final CatalogPage fromString = new CatalogPage(pageString);
		assertEquals(100,fromString.getTotal());
		assertEquals(page.getCursorId(),fromString.getCursorId());
		assertEquals(page.getIdLike(),fromString.getIdLike());
		assertNull(page.getSearchOffice());
		assertEquals(page.getCurOffice(),fromString.getCurOffice());
	}
}
