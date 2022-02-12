package cwms.radar.api;


import java.io.IOException;
import java.util.logging.Logger;

import cwms.radar.security.CwmsAccessManager;
import fixtures.RadarApiSetupCallback;
import fixtures.TestRealm;
import io.restassured.response.Response;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static cwms.radar.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
public class AccessManagerTestIT
{
	public static final Logger logger = Logger.getLogger(AccessManagerTestIT.class.getName());

	@Test
	public void can_getOne_without_user(){
		logger.info("can_getOne_without_user");

		TestRealm realm = RadarApiSetupCallback.realm();
		realm.setCurrentPrincipal(TestRealm.NULL);

		Response response = given()
				.accept("application/json;version=2")
				.queryParam("office", "SPK")
				.queryParam("names", "AR*")
				.queryParam("unit", "EN")
				.get(  "/locations");
		String bodyStrig = response.body().asString();

		response.then().assertThat()
				.statusCode(is(200));
	}

	@Test
	public void can_getOne_with_user(){
		logger.info("can_getOne_with_user");
		TestRealm realm = RadarApiSetupCallback.realm();
		realm.setCurrentPrincipal(TestRealm.USER1);

		Response response = given()
				.accept("application/json;version=2")
				.queryParam("office", "SPK")
				.queryParam("names", "AR*")
				.queryParam("unit", "EN")
				.get(  "/locations");
		String bodyStrig = response.body().asString();

		response.then().assertThat()
				.statusCode(is(200));
	}

	@Test
	public void cant_create_without_user() throws IOException
	{
		logger.info("cant_create_without_user");
		TestRealm realm = RadarApiSetupCallback.realm();
		realm.setCurrentPrincipal(TestRealm.NULL);

		String json = loadResourceAsString("cwms/radar/api/location_create.json");
		assertNotNull(json);

		final String postBody = "";
		given()
				.accept("application/json;version=2")
				.queryParam("office", "SPK")
				.body(json)
				.when()
				.post(  "/locations")
				.then()
				.assertThat().statusCode(is(401));
	}

	@Test
	public void can_create_with_user() throws IOException
	{
		logger.info("can_create_with_user");
		TestRealm realm = RadarApiSetupCallback.realm();
		realm.setCurrentPrincipal(TestRealm.USER1);

		String json = loadResourceAsString("cwms/radar/api/location_create.json");
		assertNotNull(json);

		final String postBody = "";
		given()
				.accept("application/json;version=2")
				.queryParam("office", "SPK")
				.body(json)
				.when()
				.post(  "/locations")
				.then()
				.assertThat().statusCode(is(200));
	}

	@Test
	public void cant_create_with_user_without_role() throws IOException
	{
		logger.info("cant_create_with_user_without_role");
		TestRealm realm = RadarApiSetupCallback.realm();
		realm.setCurrentPrincipal(TestRealm.USER2);

		String json = loadResourceAsString("cwms/radar/api/location_create.json");
		assertNotNull(json);

		final String postBody = "";
		given()
				.accept("application/json;version=2")
				.queryParam("office", "SPK")
				.body(json)
				.when()
				.post(  "/locations")
				.then()
				.assertThat().statusCode(is(401));
	}


}
