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

package cwms.cda.api;

import cwms.cda.api.enums.Nation;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.basin.BasinDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;

import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
class BasinControllerIT extends DataApiTestIT
{
	private static final String OFFICE = "SWT";
	private static final Basin BASIN;
	private static final Basin BASIN_CONNECT;
	private static final String DELETE_ACTION = "DELETE ALL";
	static {
		try {
			BASIN = new Basin.Builder()
					.withBasinId(new CwmsId.Builder()
							.withName("TEST_BASIN_LOCATION")
							.withOfficeId(OFFICE)
							.build())
					.withSortOrder(1.0)
					.withTotalDrainageArea(1000.0)
					.withContributingDrainageArea(500.0)
					.withAreaUnit("mi2")
					.build();

			BASIN_CONNECT = new Basin.Builder()
					.withBasinId(new CwmsId.Builder()
							.withName("TEST_BASIN_LOCATION_2")
							.withOfficeId(OFFICE)
							.build())
					.withSortOrder(2.0)
					.withTotalDrainageArea(2000.0)
					.withContributingDrainageArea(750.0)
					.withAreaUnit("mi2")
					.build();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@BeforeAll
	public static void setup() throws Exception {
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext ctx = getDslContext(c, OFFICE);
			BasinDao basinDao = new BasinDao(ctx);
			LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
			Location loc = new Location.Builder(BASIN.getBasinId().getOfficeId(), BASIN.getBasinId().getName())
					.withStateInitial("CA")
					.withNation(Nation.US)
					.withLocationKind("BASIN")
					.withCountyName("Sacramento")
					.withLatitude(38.5)
					.withLongitude(-121.5)
					.withElevation(0.0)
					.withTimeZoneName(ZoneId.of("UTC"))
					.withHorizontalDatum("WGS84")
					.withActive(true)
					.withNearestCity("Davis")
					.build();
			Location loc2 = new Location.Builder(BASIN_CONNECT.getBasinId().getOfficeId(), BASIN_CONNECT.getBasinId().getName())
					.withStateInitial("CO")
					.withNation(Nation.US)
					.withLocationKind("BASIN")
					.withCountyName("Douglas")
					.withLatitude(45.5)
					.withLongitude(-150.9)
					.withElevation(0.0)
					.withTimeZoneName(ZoneId.of("UTC"))
					.withHorizontalDatum("WGS84")
					.withActive(true)
					.withNearestCity("Denver")
					.build();
			try {
				locationsDao.storeLocation(loc);
				locationsDao.storeLocation(loc2);
				basinDao.storeBasin(BASIN_CONNECT);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}, CwmsDataApiSetupCallback.getWebUser());


	}

	@AfterAll
	public static void tearDown() throws Exception {
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext ctx = getDslContext(c, OFFICE);
			LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
			BasinDao basinDao = new BasinDao(ctx);
			locationsDao.deleteLocation(BASIN.getBasinId().getName(), OFFICE, true);
			basinDao.deleteBasin(BASIN_CONNECT.getBasinId(), DELETE_ACTION);
			locationsDao.deleteLocation(BASIN_CONNECT.getBasinId().getName(), OFFICE, true);
		}, CwmsDataApiSetupCallback.getWebUser());
	}

	@Test
	void test_get_create_delete() {

		// Test structure:
		// Create Basin
		// Retrieve Basin, assert that it exists
		// Delete Basin
		// Retrieve Basin, assert that it does not exist

		TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
		String json = Formats.format(Formats.parseHeader(Formats.JSON, Basin.class), BASIN);

		// Create basin
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.JSONV1)
			.contentType(Formats.JSONV1)
			.body(json)
			.header(AUTH_HEADER, user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("basins/")
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED))
		;

		if(BASIN.getParentBasinId() != null && BASIN.getPrimaryStreamId() != null){
			// Retrieve basin and assert that it exists
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/" + BASIN.getBasinId().getName())
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
				.body("parent-basin-id[0].name", equalTo(BASIN.getParentBasinId().getName()))
				.body("parent-basin-id[0].office-id", equalTo(BASIN.getParentBasinId().getOfficeId()))
				.body("primary-stream-id[0].name", equalTo(BASIN.getPrimaryStreamId().getName()))
				.body("primary-stream-id[0].office-id", equalTo(BASIN.getPrimaryStreamId().getOfficeId()))
			;
		} else if (BASIN.getPrimaryStreamId() != null) {
			// Retrieve basin and assert that it exists
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/" + BASIN.getBasinId().getName())
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
				.body("primary-stream-id[0].name", equalTo(BASIN.getPrimaryStreamId().getName()))
				.body("primary-stream-id[0].office-id", equalTo(BASIN.getPrimaryStreamId().getOfficeId()))
			;
		} else if (BASIN.getParentBasinId() != null) {
			// Retrieve basin and assert that it exists
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/" + BASIN.getBasinId().getName())
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
				.body("parent-basin-id[0].name", equalTo(BASIN.getParentBasinId().getName()))
				.body("parent-basin-id[0].office-id", equalTo(BASIN.getParentBasinId().getOfficeId()))
			;
		} else {
			// Retrieve basin and assert that it exists
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/" + BASIN.getBasinId().getName())
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
			;
		}

		// Delete basin
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.JSONV1)
			.queryParam(Controllers.OFFICE, OFFICE)
			.queryParam(DELETE_MODE, DELETE_ACTION)
			.header(AUTH_HEADER, user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.delete("basins/" + BASIN.getBasinId().getName())
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT))
		;

		// Retrieve basin and assert that it does not exist
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.JSONV1)
			.queryParam(Controllers.OFFICE, OFFICE)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("basins/" + BASIN.getBasinId().getName())
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND))
		;
	}

	@Test
	void test_update_does_not_exist() {
		String json = Formats.format(Formats.parseHeader(Formats.JSON, Basin.class), BASIN);
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
		// Create basin
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.JSONV1)
			.contentType(Formats.JSONV1)
			.queryParam(NAME, "newFakeName")
			.body(json)
			.header(AUTH_HEADER, user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.patch("basins/oldFakeName")
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND))
		;
	}

	@Test
	void test_delete_does_not_exist() {
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
		// Delete basin
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.JSONV1)
			.queryParam(Controllers.OFFICE, OFFICE)
			.queryParam(DELETE_MODE, DELETE_ACTION)
			.header(AUTH_HEADER, user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.delete("basins/" + Instant.now().toEpochMilli())
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NOT_FOUND))
		;
	}

	@Test
	void test_get_all() {

		// Test structure:
		// Create Basin
		// Retrieve all Basins, assert that the created basin is in the list
		// Delete Basin

		TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
		String json = Formats.format(Formats.parseHeader(Formats.JSON, Basin.class), BASIN);

		// Create basin
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.JSONV1)
			.contentType(Formats.JSONV1)
			.body(json)
			.header(AUTH_HEADER, user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("basins/")
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED))
		;


		if (BASIN.getParentBasinId() != null && BASIN.getPrimaryStreamId() != null) {
			// Retrieve all basins and assert that the created basin is in the list
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/")
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
				.body("parent-basin-id[0].name", equalTo(BASIN.getParentBasinId().getName()))
				.body("parent-basin-id[0].office-id", equalTo(BASIN.getParentBasinId().getOfficeId()))
				.body("primary-stream-id[0].name", equalTo(BASIN.getPrimaryStreamId().getName()))
				.body("primary-stream-id[0].office-id", equalTo(BASIN.getPrimaryStreamId().getOfficeId()))
			;
		} else if (BASIN.getPrimaryStreamId() != null) {
			// Retrieve all basins and assert that the created basin is in the list
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/")
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
				.body("primary-stream-id[0].name", equalTo(BASIN.getPrimaryStreamId().getName()))
				.body("primary-stream-id[0].office-id", equalTo(BASIN.getPrimaryStreamId().getOfficeId()))
			;
		} else if (BASIN.getParentBasinId() != null) {
			// Retrieve all basins and assert that the created basin is in the list
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/")
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
				.body("parent-basin-id[0].name", equalTo(BASIN.getParentBasinId().getName()))
				.body("parent-basin-id[0].office-id", equalTo(BASIN.getParentBasinId().getOfficeId()))
			;
		} else {
			// Retrieve all basins and assert that the created basin is in the list
			given()
				.log().ifValidationFails(LogDetail.ALL, true)
				.accept(Formats.JSONV1)
				.queryParam(Controllers.OFFICE, OFFICE)
			.when()
				.redirects().follow(true)
				.redirects().max(3)
				.get("basins/")
			.then()
				.log().ifValidationFails(LogDetail.ALL, true)
			.assertThat()
				.statusCode(is(HttpServletResponse.SC_OK))
				.body("basin-id[0].name", equalTo(BASIN.getBasinId().getName()))
				.body("basin-id[0].office-id", equalTo(BASIN.getBasinId().getOfficeId()))
				.body("[0].sort-order", equalTo(BASIN.getSortOrder().floatValue()))
				.body("[0].area-unit", equalTo(BASIN.getAreaUnit()))
				.body("[0].total-drainage-area", equalTo(BASIN.getTotalDrainageArea().floatValue()))
				.body("[0].contributing-drainage-area", equalTo(BASIN.getContributingDrainageArea().floatValue()))
			;
		}

		// Delete basin
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.JSONV1)
			.queryParam(Controllers.OFFICE, OFFICE)
			.queryParam(DELETE_MODE, DELETE_ACTION)
			.header(AUTH_HEADER, user.toHeaderValue())
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.delete("basins/" + BASIN.getBasinId().getName())
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT))
		;
	}

	@Test
	void test_get_all_connectivity() {
		// Retrieve all basins using legacy getter and assert that the created basin is in the list
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.NAMED_PGJSON)
			.queryParam(Controllers.OFFICE, OFFICE)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("basins/")
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("name", equalTo(BASIN_CONNECT.getBasinId().getName()))
		;
	}

	@Test
	void test_get_one_connectivity() {
		// Retrieve basin using legacy getter and assert that the created basin exists
		given()
			.log().ifValidationFails(LogDetail.ALL, true)
			.accept(Formats.NAMED_PGJSON)
			.queryParam(Controllers.OFFICE, OFFICE)
		.when()
			.redirects().follow(true)
			.redirects().max(3)
			.get("basins/" + BASIN_CONNECT.getBasinId().getName())
		.then()
			.log().ifValidationFails(LogDetail.ALL, true)
		.assertThat()
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("name", equalTo(BASIN_CONNECT.getBasinId().getName()))
		;

	}
}