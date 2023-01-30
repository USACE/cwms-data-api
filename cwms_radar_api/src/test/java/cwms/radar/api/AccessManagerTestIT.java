package cwms.radar.api;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import fixtures.TestAccounts.KeyUser;
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
	private static KeyUser SPK_NORMAL_USER = KeyUser.SPK_NORMAL;
	private static KeyUser SPK_NO_ROLES_USER = KeyUser.SPK_NO_ROLES;

	@Test
	public void can_getOne_without_user(){
		Response response = given()
				.contentType("application/json")
				.queryParam("office", "SPK")
				.queryParam("names", "AR*")
				.queryParam("unit", "EN")
				.get(  "/locations");

		response.then().assertThat()
				.statusCode(is(200));
	}

	@Test
	public void can_getOne_with_user(){
		Response response = given()
				.contentType("application/json")
				.queryParam("office", "SPK")
				.queryParam("names", "AR*")
				.queryParam("unit", "EN")
				.header("Authorization",SPK_NORMAL_USER.toHeaderValue())
				.get(  "/locations");

		response.then().assertThat()
				.statusCode(is(200));
	}

	@Test
	public void cant_create_without_user() throws IOException
	{
		String json = loadResourceAsString("cwms/radar/api/location_create.json");
		assertNotNull(json);

		given()
				.contentType("application/json")
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
		String json = loadResourceAsString("cwms/radar/api/location_create_spk.json");
		assertNotNull(json);


		given()
				.contentType("application/json")
				.queryParam("office", "SPK")
				.header("Authorization",SPK_NORMAL_USER.toHeaderValue())
				.body(json)
				.when()
				.post(  "/locations")
				.then()
				.assertThat().statusCode(HttpServletResponse.SC_ACCEPTED);
	}

	@Test
	public void cant_create_with_user_without_role() throws IOException
	{
		String json = loadResourceAsString("cwms/radar/api/location_create.json");
		assertNotNull(json);

		final String postBody = "";
		given()
				.contentType("application/json")
				.queryParam("office", "SPK")
				.header("Authorization",SPK_NO_ROLES_USER.toHeaderValue())
				.body(json)
				.when()
				.post(  "/locations")
				.then()
				.assertThat().statusCode(is(403));
	}


}
