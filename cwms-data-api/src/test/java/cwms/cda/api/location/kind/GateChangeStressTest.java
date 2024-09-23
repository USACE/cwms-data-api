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

package cwms.cda.api.location.kind;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.api.enums.Unit;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.BaseOutletDaoIT;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dao.location.kind.ProjectStructureIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.GateChange;
import cwms.cda.data.dto.location.kind.GateSetting;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.metadata.constants.NumericalConstants;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

class GateChangeStressTest extends BaseOutletDaoIT {

    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final CwmsId PROJECT_ID = new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("PROJECT3").build();
    private static final Location PROJECT_LOCATION = buildProjectLocation(PROJECT_ID.getName());
    private static final String CONDUIT_GATE_RATING_SPEC_ID = PROJECT_ID.getName() + ".Opening-ConduitGate,Elev;Flow-ConduitGate.Standard.Production";
    private static final CwmsId CONDUIT_GATE_RATING_GROUP = new CwmsId.Builder()
            .withName("Rating-" + PROJECT_ID.getName() + "-ConduitGate")
            .withOfficeId(OFFICE_ID)
            .build();

    private static final TemporalAmount INTERVAL = Duration.of(5, ChronoUnit.MINUTES);
    private static final ZonedDateTime START = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, NumericalConstants.UTC_ZONEID);
    private static final ZonedDateTime END = START.plusYears(1).plus(INTERVAL);

    @BeforeAll
    public static void setup() throws Exception {
        //1 year of 5 minute gate change data for 18 gates
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), buildProject(PROJECT_LOCATION), "T");

            OutletDao dao = new OutletDao(context);
            try {
                dao.retrieveOperationalChanges(PROJECT_ID, END.minus(INTERVAL).toInstant(), END.toInstant(), true, true,
                                               UnitSystem.EN, 2);
                LOGGER.atSevere().log("Found existing operation changes.  Not attempting to generate and store changes again.");
                return;
            } catch (NotFoundException ex) {
                LOGGER.atSevere().log("Could not find any changes at the end of the period.  Generating and storing changes.");
            }

            String locPrefix = PROJECT_ID.getName() + "-CG";
            CwmsId.Builder idBuilder = new CwmsId.Builder().withOfficeId(OFFICE_ID);
            List<CwmsId> outletIds = IntStream.rangeClosed(1, 18)
                                              .mapToObj(i -> locPrefix + String.format("%03d", i))
                                              .map(name -> idBuilder.withName(name).build())
                                              .collect(Collectors.toList());
            List<Location> allLocs = outletIds.stream()
                                              .map(CwmsId::getName)
                                              .map(ProjectStructureIT::buildProjectLocation)
                                              .collect(Collectors.toList());
            Outlet.Builder outBuilder = new Outlet.Builder().withRatingGroupId(CONDUIT_GATE_RATING_GROUP)
                                                            .withProjectId(PROJECT_ID);
            List<Outlet> outlets = allLocs.stream()
                                          .map(loc -> outBuilder.withLocation(loc).build())
                                          .collect(Collectors.toList());

            List<List<Instant>> dateBatches = buildBatchDates();
            LookupType compType = new LookupType.Builder().withActive(true)
                                                          .withDisplayValue("A")
                                                          .withOfficeId("CWMS")
                                                          .withTooltip("Adjusted by an automated method")
                                                          .build();
            LookupType releaseReason = new LookupType.Builder().withActive(true)
                                                               .withDisplayValue("O")
                                                               .withOfficeId("CWMS")
                                                               .withTooltip("Other release")
                                                               .build();
            GateSetting.Builder settingBuilder = new GateSetting.Builder().withOpening(15.)
                                                                          .withOpeningParameter("Elev")
                                                                          .withInvertElevation(5.)
                                                                          .withOpeningUnits("ft");
            GateChange.Builder changeBuilder = new GateChange.Builder().withProjectId(PROJECT_ID)
                                                                       .withDischargeComputationType(compType)
                                                                       .withReasonType(releaseReason)
                                                                       .withProtected(false)
                                                                       .withNewTotalDischargeOverride(1.5)
                                                                       .withOldTotalDischargeOverride(2.0)
                                                                       .withDischargeUnits("cfs")
                                                                       .withPoolElevation(50.)
                                                                       .withTailwaterElevation(30.)
                                                                       .withElevationUnits("ft")
                                                                       .withNotes("Notes");
            List<List<GateChange>> changes = dateBatches.stream()
                                                        .map(dates -> buildGateChanges(dates, changeBuilder, settingBuilder,
                                                                                       outletIds))
                                                        .collect(Collectors.toList());

            LOGGER.atSevere().log("Storing locations...");
            allLocs.forEach(loc -> storeLocLogException(context, loc));
            LOGGER.atSevere().log("Storing outlets...");
            outlets.forEach(outlet -> storeOutlet(context, outlet));
            LOGGER.atSevere().log("Storing rating spec id...");
            createRatingSpecForOutlet(context, outlets.get(0), CONDUIT_GATE_RATING_SPEC_ID);

            LOGGER.atSevere().log("Storing changes...");
            changes.forEach(changeBatch -> {
                LOGGER.atSevere().log("Storing " + changeBatch.size() + " changes from " + changeBatch.get(0).getChangeDate() + " to " + changeBatch.get(changeBatch.size() - 1));
                dao.storeOperationalChanges(changeBatch, true);
            });

        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void stress_test() {
        //Generate 18 outlets, 5 minute data, for 1 year
        //location: keystone dam
        //figure out how big the payload is.
        int pageSize = (int) (TimeUnit.DAYS.toMinutes(14) / 15);

        LOGGER.atInfo().log("Retrieving changes with page size: " + pageSize);

        String body = given().log()
                             .ifValidationFails(LogDetail.ALL, true)
                             .contentType(Formats.JSONV1)
                         .when()
                             .redirects()
                             .follow(true)
                             .redirects()
                             .max(3)
                             .queryParam(BEGIN, START.toInstant().toString())
                             .queryParam(Controllers.END, END.toInstant().toString())
                             .queryParam(START_TIME_INCLUSIVE, true)
                             .queryParam(END_TIME_INCLUSIVE, true)
                             .queryParam(PAGE_SIZE, pageSize)
                             .get("projects/" + OFFICE_ID + "/" + PROJECT_ID.getName() + "/gate-changes")
                         .then()
                             .log()
                             .ifValidationFails(LogDetail.ALL, true)
                             .assertThat()
                             .statusCode(is(HttpServletResponse.SC_OK))
                             .extract()
                             .body()
                             .asString();

        assertNotNull(body);
        assertFalse(body.isEmpty());

        LOGGER.atSevere().log("Output character length: " + body.length() + "\n" +
                                      Charset.defaultCharset().displayName() + " byte length: " + body.getBytes().length);
    }

    private static List<List<Instant>> buildBatchDates() {
        TemporalAmount batch = Period.ofWeeks(2);
        ZonedDateTime batchStart = START;
        ZonedDateTime batchEnd = START.plus(batch);
        List<List<Instant>> dateBatches = new ArrayList<>();

        while (batchStart.isBefore(END)) {
            List<Instant> times = new ArrayList<>();
            dateBatches.add(times);
            ZonedDateTime nextInterval = batchStart;

            while (nextInterval.isBefore(batchEnd)) {
                times.add(nextInterval.toInstant());
                nextInterval = nextInterval.plus(INTERVAL);
            }

            batchStart = batchStart.plus(batch);
            batchEnd = batchEnd.plus(batch);
        }
        return dateBatches;
    }

    private static List<GateChange> buildGateChanges(List<Instant> dates, GateChange.Builder changeBuilder, GateSetting.Builder settingBuilder, List<CwmsId> outletIds) {
        return dates.stream().map(date -> {
            List<GateSetting> settings = outletIds.stream()
                                                  .map(id -> settingBuilder.withLocationId(id).build())
                                                  .collect(Collectors.toList());
            return changeBuilder.withChangeDate(date).withSettings(settings).build();
        }).collect(Collectors.toList());
    }

    private static void storeLocLogException(DSLContext context, Location loc) throws RuntimeException {
        LocationsDaoImpl dao = new LocationsDaoImpl(context);
        String locName = loc.getOfficeId() + "." + loc.getName();
        try {
            dao.getLocation(loc.getName(), UnitSystem.EN.getValue(), OFFICE_ID);
            LOGGER.atInfo().log("Location already exists: " + locName);
        } catch (NotFoundException ex) {
            LOGGER.atInfo().log("No location found for " + locName + " storing it.");
            try {
                dao.storeLocation(loc);
            } catch (IOException ex2) {
                String msg = "Unable to store location data for " + locName;
                LOGGER.atSevere().log(msg);
                throw new RuntimeException(msg, ex2);
            }
        }
    }
}
