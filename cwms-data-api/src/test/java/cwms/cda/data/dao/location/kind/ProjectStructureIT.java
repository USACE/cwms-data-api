/*
 * Copyright (c) 2024. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 */

package cwms.cda.data.dao.location.kind;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.location.kind.Outlet;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public abstract class ProjectStructureIT extends DataApiTestIT {
	private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
	protected static final TestAccounts.KeyUser USER = TestAccounts.KeyUser.SPK_NORMAL;
	protected static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();
	public static final Location PROJECT_LOC = buildProjectLocation("PROJECT1");
	public static final Location PROJECT_LOC2 = buildProjectLocation("PROJECT2");
	public static final CwmsId PROJECT_1_ID = new CwmsId.Builder().withName(PROJECT_LOC.getName())
																   .withOfficeId(PROJECT_LOC.getOfficeId())
																   .build();
	public static final CwmsId PROJECT_2_ID = new CwmsId.Builder().withName(PROJECT_LOC2.getName())
																   .withOfficeId(PROJECT_LOC2.getOfficeId())
																   .build();

	public static void setupProject() throws Exception {
		//Don't tag this as a @BeforeAll - JUnit can't guarantee this occurs first.
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, OFFICE_ID);
			CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), buildProject(PROJECT_LOC), "T");
			CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), buildProject(PROJECT_LOC2), "T");
		}, CwmsDataApiSetupCallback.getWebUser());
	}

	public static void tearDownProject() throws Exception
	{
		//Don't tag this as a @AfterAll - JUnit can't guarantee this occurs first.
		CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
		databaseLink.connection(c -> {
			DSLContext context = getDslContext(c, OFFICE_ID);
			LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
			CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC.getName(),
					DeleteRule.DELETE_ALL.getRule(), OFFICE_ID);
			CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC2.getName(),
					DeleteRule.DELETE_ALL.getRule(), OFFICE_ID);
			locationsDao.deleteLocation(PROJECT_LOC.getName(), OFFICE_ID, true);
			locationsDao.deleteLocation(PROJECT_LOC2.getName(), OFFICE_ID, true);
		}, CwmsDataApiSetupCallback.getWebUser());
	}

	public static Location buildProjectLocation(String locationId) {
		return new Location.Builder(locationId, "PROJECT", ZoneId.of("UTC"),
				38.5613824, -121.7298432, "NVGD29", OFFICE_ID)
				.withElevation(10.0)
				.withElevationUnits("m")
				.withLocationType("SITE")
				.withCountyName("Sacramento")
				.withNation(Nation.US)
				.withActive(true)
				.withStateInitial("CA")
				.withPublishedLatitude(38.5613824)
				.withPublishedLongitude(-121.7298432)
				.withBoundingOfficeId(OFFICE_ID)
				.withLongName("UNITED STATES")
				.withDescription("for testing")
				.withNearestCity("Davis")
				.build();
	}

	public static PROJECT_OBJ_T buildProject(Location location) {
		PROJECT_OBJ_T retval = new PROJECT_OBJ_T();
		retval.setPROJECT_LOCATION(LocationUtil.getLocation(location));
		retval.setPUMP_BACK_LOCATION(null);
		retval.setNEAR_GAGE_LOCATION(null);
		retval.setAUTHORIZING_LAW(null);
		retval.setCOST_YEAR(Timestamp.from(Instant.now()));
		retval.setFEDERAL_COST(BigDecimal.ONE);
		retval.setNONFEDERAL_COST(BigDecimal.TEN);
		retval.setFEDERAL_OM_COST(BigDecimal.ZERO);
		retval.setNONFEDERAL_OM_COST(BigDecimal.valueOf(15.0));
		retval.setCOST_UNITS_ID("$");
		retval.setREMARKS("TEST RESERVOIR PROJECT");
		retval.setPROJECT_OWNER("CDA");
		retval.setHYDROPOWER_DESCRIPTION("HYDRO DESCRIPTION");
		retval.setSEDIMENTATION_DESCRIPTION("SEDIMENTATION DESCRIPTION");
		retval.setDOWNSTREAM_URBAN_DESCRIPTION("DOWNSTREAM URBAN DESCRIPTION");
		retval.setBANK_FULL_CAPACITY_DESCRIPTION("BANK FULL CAPACITY DESCRIPTION");
		retval.setYIELD_TIME_FRAME_START(Timestamp.from(Instant.now()));
		retval.setYIELD_TIME_FRAME_END(Timestamp.from(Instant.now()));
		return retval;
	}

	public static Location buildProjectStructureLocation(String locationId, String locationKind) {
		return new Location.Builder(locationId, locationKind, ZoneId.of("UTC"),
			38.5613824, -121.7298432, "NVGD29", OFFICE_ID)
			.withPublicName("Integration Test " + locationId)
			.withLongName("Integration Test " + locationId + " " + locationKind)
			.withElevation(10.0)
			.withElevationUnits("m")
			.withLocationType("SITE")
			.withCountyName("Sacramento")
			.withNation(Nation.US)
			.withActive(true)
			.withStateInitial("CA")
			.withBoundingOfficeId(OFFICE_ID)
			.withPublishedLatitude(38.5613824)
			.withPublishedLongitude(-121.7298432)
			.withLongName("UNITED STATES")
			.withDescription("for testing")
			.withNearestCity("Davis")
			.build();
	}

	public static void storeLocation(DSLContext context, Location loc) throws IOException {
		LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
		deleteLocation(context, loc.getOfficeId(), loc.getName());
		locationsDao.storeLocation(loc);
	}

	public static void deleteLocation(DSLContext context, String officeId, String locId) {
		LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
		try {
			locationsDao.deleteLocation(locId, officeId, true);
		} catch (NotFoundException ex) {
			LOGGER.atFinest().withCause(ex).log("No data found for " + officeId + "." + locId);
		}
	}

	public static void deleteLocationGroup(DSLContext context, Outlet outlet) {
		LocationGroupDao locationGroupDao = new LocationGroupDao(context);
		try {
			locationGroupDao.delete(outlet.getRatingCategoryId().getName(), outlet.getRatingGroupId().getName(), true, OFFICE_ID);
		} catch (NotFoundException e) {
			LOGGER.atFinest().withCause(e).log("No data found for category:" + outlet.getRatingCategoryId().getName()
													   + ", group-id:" + outlet.getRatingGroupId().getName());
		}
	}

	public static <T extends CwmsDTOBase> void containsDto(List<T> outlets, T expectedDto, BiPredicate<T, T> identifier, BiConsumer<T, T> dtoMatcher) {
		T receivedOutlet = outlets.stream()
									   .filter(dto -> identifier.test(dto, expectedDto))
									   .findFirst()
									   .orElse(null);
		assertNotNull(receivedOutlet);
		dtoMatcher.accept(expectedDto, receivedOutlet);
	}

	public static <T extends CwmsDTOBase> void doesNotContainDto(List<T> outlets, T expectedDto, BiPredicate<T, T> identifier) {
		T receivedDto = outlets.stream()
									   .filter(dto -> identifier.test(dto, expectedDto))
									   .findFirst()
									   .orElse(null);
		assertNull(receivedDto);
	}

	public static void createRatingSpecForOutlet(DSLContext context, Outlet outlet, String specId) {
		LocationGroupDao locGroupDao = new LocationGroupDao(context);
		Optional<LocationGroup> ratingGroup = locGroupDao.getLocationGroup(
				outlet.getRatingCategoryId().getOfficeId(),
				outlet.getRatingCategoryId().getName(),
				outlet.getRatingGroupId().getName());

		if (ratingGroup.isPresent()) {
			LocationGroup realGroup = ratingGroup.get();
			List<AssignedLocation> assLocs = realGroup.getAssignedLocations();
			realGroup = new LocationGroup(realGroup.getLocationCategory(), realGroup.getOfficeId(),
										  realGroup.getId(), realGroup.getDescription(),
										  specId, realGroup.getSharedRefLocationId(),
										  realGroup.getLocGroupAttribute());
			realGroup = new LocationGroup(realGroup, assLocs);
			locGroupDao.delete(realGroup.getLocationCategory().getId(), realGroup.getId(), true,
							   realGroup.getOfficeId());
			locGroupDao.create(realGroup);
		}
	}
}
