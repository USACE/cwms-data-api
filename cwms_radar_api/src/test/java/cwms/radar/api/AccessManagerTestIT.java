package cwms.radar.api;

import java.io.IOException;
import java.util.logging.Logger;

import fixtures.RadarApiSetupCallback;
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
				.header("Authorization","name user1")
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
		String json = loadResourceAsString("cwms/radar/api/location_create.json");
		assertNotNull(json);


		given()
				.contentType("application/json")
				.queryParam("office", "SPK")
				.header("Authorization","name user1")
				.body(json)
				.when()
				.post(  "/locations")
				.then()
				.assertThat().statusCode(202);
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
				.header("Authorization","name user2")
				.body(json)
				.when()
				.post(  "/locations")
				.then()
				.assertThat().statusCode(is(403));
	}


}
