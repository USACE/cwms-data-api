/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

package cwms.cda.data.dao;

import static cwms.cda.api.Controllers.BOUNDING_OFFICE_LIKE;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.LOCATION_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.LOCATION_GROUP_LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.data.dao.DeleteRule.DELETE_LOC;
import static cwms.cda.data.dao.DeleteRule.DELETE_LOC_CASCADE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC.AV_LOC;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.enums.Unit;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.catalog.CatalogEntry;
import cwms.cda.data.dto.catalog.LocationAlias;
import cwms.cda.data.dto.catalog.LocationCatalogEntry;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.Point;
import org.jetbrains.annotations.NotNull;
import org.jooq.CommonTableExpression;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.SelectSeekStep3;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.dao.ifc.loc.CwmsDbLoc;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC2;


public class LocationsDaoImpl extends JooqDao<Location> implements LocationsDao {
    private static final Logger logger = Logger.getLogger(LocationsDaoImpl.class.getName());
    private static final long DELETED_TS_MARKER = 0L;

    public LocationsDaoImpl(DSLContext dsl) {
        super(dsl);
    }


    @Override
    public String getLocations(String names, String format, String units, String datum,
                               String officeId) {

        return CWMS_LOC_PACKAGE.call_RETRIEVE_LOCATIONS_F(dsl.configuration(),
                names, format, units, datum, officeId);
    }

    @Override
    public Location getLocation(String locationName, String unitSystem, String officeId) {
        Record loc = dsl.select(AV_LOC.asterisk())
                .from(AV_LOC)
                .where(AV_LOC.DB_OFFICE_ID.equalIgnoreCase(officeId)
                        .and(AV_LOC.UNIT_SYSTEM.equalIgnoreCase(unitSystem)
                                .and(AV_LOC.LOCATION_ID.equalIgnoreCase(locationName))))
                .fetchOne();
        if (loc == null) {
            throw new NotFoundException("Location not found for office:" + officeId + " and unit "
                    + "system:" + unitSystem + " and id:" + locationName);
        }
        return buildLocation(loc);
    }

    private Location buildLocation(Record loc) {
        String timeZoneName = loc.get(AV_LOC.TIME_ZONE_NAME); // may be null...
        ZoneId zone = null;
        if (timeZoneName != null) {
            zone = ZoneId.of(timeZoneName);
        }

        Double latDouble = null;
        BigDecimal latBigDec = loc.get(AV_LOC.LATITUDE);
        if (latBigDec != null) {
            latDouble = latBigDec.doubleValue();
        }

        Double longDouble = null;
        BigDecimal longBigDec = loc.get(AV_LOC.LONGITUDE);
        if (longBigDec != null) {
            longDouble = longBigDec.doubleValue();
        }

        Location.Builder locationBuilder = new Location.Builder(
                loc.get(AV_LOC.LOCATION_ID),
                loc.get(AV_LOC.LOCATION_KIND_ID),
                zone,
                latDouble,
                longDouble,
                loc.get(AV_LOC.HORIZONTAL_DATUM),
                loc.get(AV_LOC.DB_OFFICE_ID)
        )
                .withLocationType(loc.get(AV_LOC.LOCATION_TYPE))
                .withElevation(loc.get(AV_LOC.ELEVATION))
                .withVerticalDatum(loc.get(AV_LOC.VERTICAL_DATUM))
                .withPublicName(loc.get(AV_LOC.PUBLIC_NAME))
                .withLongName(loc.get(AV_LOC.LONG_NAME))
                .withDescription(loc.get(AV_LOC.DESCRIPTION))
                .withCountyName(loc.get(AV_LOC.COUNTY_NAME))
                .withStateInitial(loc.get(AV_LOC.STATE_INITIAL))
                .withActive(loc.get(AV_LOC.ACTIVE_FLAG).equalsIgnoreCase("T"))
                .withMapLabel(loc.get(AV_LOC.MAP_LABEL))
                .withBoundingOfficeId(loc.get(AV_LOC.BOUNDING_OFFICE_ID))
                .withNearestCity(loc.get(AV_LOC.NEAREST_CITY))
                .withNation(Nation.nationForName(loc.get(AV_LOC.NATION_ID)));

        BigDecimal pubLatitude = loc.get(AV_LOC.PUBLISHED_LATITUDE);
        BigDecimal pubLongitude = loc.get(AV_LOC.PUBLISHED_LONGITUDE);
        if (pubLatitude != null) {
            locationBuilder.withPublishedLatitude(pubLatitude.doubleValue());
        }
        if (pubLongitude != null) {
            locationBuilder.withPublishedLongitude(pubLongitude.doubleValue());
        }
        return locationBuilder.build();
    }

    @Override
    public void deleteLocation(String locationName, String officeId) {        
        deleteLocation(locationName, officeId, false);
    }

    @Override
    public void deleteLocation(String locationName, String officeId, boolean cascadeDelete) {
        dsl.connection(c -> {
            Configuration configuration = getDslContext(c, officeId).configuration();
            if (cascadeDelete) {
                CWMS_LOC_PACKAGE.call_DELETE_LOCATION(configuration, locationName, DELETE_LOC_CASCADE.getRule(), officeId);
            } else {
                CWMS_LOC_PACKAGE.call_DELETE_LOCATION(configuration, locationName, DELETE_LOC.getRule(), officeId);
            }
        });
    }

    @Override
    public void storeLocation(Location location) throws IOException {
        location.validate();
        try {
            connection(dsl, c -> {
                setOffice(c,location);
                CwmsDbLoc locJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLoc.class, c);
                String elevationUnits = Unit.METER.getValue();
                locJooq.store(c, location.getOfficeId(), location.getName(),
                        location.getStateInitial(), location.getCountyName(),
                        location.getTimezoneName(), location.getLocationType(),
                        location.getLatitude(), location.getLongitude(), location.getElevation(),
                        elevationUnits, location.getVerticalDatum(),
                        location.getHorizontalDatum(), location.getPublicName(),
                        location.getLongName(),
                        location.getDescription(), location.getActive(),
                        location.getLocationKind(), location.getMapLabel(),
                        location.getPublishedLatitude(),
                        location.getPublishedLongitude(), location.getBoundingOfficeId(),
                        location.getNation().getName(), location.getNearestCity(), true);

            });
        } catch (DataAccessException ex) {
            throw new IOException("Failed to store Location", ex);
        }
    }

    @Override
    public void renameLocation(String oldLocationName, Location renamedLocation)
            throws IOException {
        renamedLocation.validate();
        try {
            connection(dsl, c -> {
                setOffice(c,renamedLocation);
                CwmsDbLoc locJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLoc.class, c);
                String elevationUnits = Unit.METER.getValue();
                locJooq.rename(c, renamedLocation.getOfficeId(), oldLocationName,
                        renamedLocation.getName(), renamedLocation.getStateInitial(),
                        renamedLocation.getCountyName(), renamedLocation.getTimezoneName(),
                        renamedLocation.getLocationType(),
                        renamedLocation.getLatitude(), renamedLocation.getLongitude(),
                        renamedLocation.getElevation(), elevationUnits,
                        renamedLocation.getVerticalDatum(), renamedLocation.getHorizontalDatum(),
                        renamedLocation.getPublicName(),
                        renamedLocation.getLongName(), renamedLocation.getDescription(),
                        renamedLocation.getActive(), true);
            });
        } catch (DataAccessException ex) {
            throw new IOException("Failed to rename Location", ex);
        }
    }

    @Override
    public FeatureCollection buildFeatureCollection(String names, String units, String officeId) {
        if (!"EN".equals(units)) {
            units = "SI";
        }

        SelectConditionStep<Record> selectQuery = dsl.select(asterisk())
                .from(AV_LOC)
                .where(AV_LOC.DB_OFFICE_ID.eq(officeId))
                .and(AV_LOC.UNIT_SYSTEM.eq(units));

        if (names != null && !names.isEmpty()) {
            List<String> identifiers = new ArrayList<>();
            if (names.contains("|")) {
                String[] namePieces = names.split("\\|");
                identifiers.addAll(Arrays.asList(namePieces));
            } else {
                identifiers.add(names);
            }

            selectQuery = selectQuery.and(AV_LOC.LOCATION_ID.in(identifiers));
        }

        List<Feature> features = selectQuery.stream()
                .map(LocationsDaoImpl::buildFeatureFromAvLocRecord)
                .collect(toList());
        FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(features);

        return collection;
    }

    public static Feature buildFeatureFromAvLocRecord(Record avLocRecord) {
        Feature feature = new Feature();

        String featureId = avLocRecord.getValue(AV_LOC.PUBLIC_NAME, String.class);
        if (featureId == null || featureId.isEmpty()) {
            featureId = avLocRecord.getValue(AV_LOC.LOCATION_ID, String.class);
        }
        feature.setId(featureId);

        Double longitude = avLocRecord.getValue(AV_LOC.LONGITUDE, Double.class);
        Double latitude = avLocRecord.getValue(AV_LOC.LATITUDE, Double.class);

        if (latitude == null) {
            latitude = 0.0;
        }

        if (longitude == null) {
            longitude = 0.0;
        }

        feature.setGeometry(new Point(longitude, latitude));

        Map<String, Object> recordMap = avLocRecord.intoMap();
        List<String> keysWithNullValue =
                recordMap.entrySet().stream().filter(e -> e.getValue() == null)
                        .map(Map.Entry::getKey).collect(toList());
        keysWithNullValue.forEach(recordMap::remove);
        recordMap.remove(AV_LOC.LATITUDE.getName());
        recordMap.remove(AV_LOC.LONGITUDE.getName());
        recordMap.remove(AV_LOC.PUBLIC_NAME.getName());

        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("avLoc", recordMap);
        feature.setProperties(properties);

        return feature;
    }


    @Override
    public Catalog getLocationCatalog(String page, int pageSize, String unitSystem, String office,
                                      String idLike, String categoryLike, String groupLike, String boundingOfficeLike) {

        // Parse provided page and pull out the parameters

        Catalog.CatalogPage catPage = null;
        if (page != null && !page.isEmpty()) {
            catPage = new Catalog.CatalogPage(page);

            // The cursor urlencodes the initial query parameters, We should decode them and use the cursor values.
            // If the user provides a page parameter and query parameters they should match.
            // If they don't match its weird and we will log it.
            office = warnIfMismatch(OFFICE, catPage.getSearchOffice(), office);
            idLike = warnIfMismatch(LIKE, catPage.getIdLike(), idLike);
            categoryLike = warnIfMismatch(LOCATION_CATEGORY_LIKE, catPage.getLocCategoryLike(), categoryLike);
            groupLike = warnIfMismatch(LOCATION_GROUP_LIKE, catPage.getLocGroupLike(), groupLike);
            boundingOfficeLike = warnIfMismatch(BOUNDING_OFFICE_LIKE, catPage.getBoundingOfficeLike(), boundingOfficeLike);
        }

        return getLocationCatalog(catPage, pageSize, unitSystem, office, idLike, categoryLike, groupLike, boundingOfficeLike);
    }

    private Catalog getLocationCatalog(Catalog.CatalogPage catPage, int pageSize, String unitSystem, String office,
                                      String idLike, String categoryLike, String groupLike, String boundingOfficeLike) {

        final AV_LOC2 avLoc2 = AV_LOC2.AV_LOC2;  // ref the view just shorten the jooq
        //Now querying against AV_LOC2 as it gives us back the same information as querying against
        //location group views. This makes the code clearer and improves performance.
        //If there is a performance improvement by switching back to location groups and querying against
        //location codes (previous implementation used location_id) for joins, feel free to implement.
        Objects.requireNonNull(idLike, "A value must be provided for the idLike field. Specifiy .* if you don't care.");

        // "condition" needs to be used by the count query and the results query.
        Condition condition = buildWhereCondition(unitSystem, office, idLike, categoryLike, groupLike, boundingOfficeLike);

        int total;
        String cursorLocation; // The location-id of the cursor in the results
        String cursorOffice; // If the user did not provide a value in the "office" filter then
        // results may contain locations from multiple offices. cursorOffice will track the office
        // of the cursor in the results.
        if (catPage == null) {
            cursorLocation = "*";
            cursorOffice = null;

            SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                .from(avLoc2)
                .where(condition);
            logger.log(Level.FINER, () -> count.getSQL(ParamType.INLINED));
            total = count.fetchOne().value1();
        } else {
            cursorLocation = catPage.getCursorId();
            cursorOffice = catPage.getCurOffice();

            total = catPage.getTotal();
            pageSize = catPage.getPageSize();
        }

        condition = addCursorConditions(condition, cursorOffice, cursorLocation);

        Field<String> dataId = avLoc2.LOCATION_ID.as("real_id");
        Field<Long> dataCode = avLoc2.LOCATION_CODE.as("real_code");
        // data/limiter/query
        Table<?> data = dsl.select(dataId,dataCode)
                           .from(avLoc2)
                           .where(condition.and(avLoc2.ALIASED_ITEM.isNull()))
                           .orderBy(avLoc2.DB_OFFICE_ID.asc(),avLoc2.LOCATION_ID.asc())
                           .asTable("data");
        CommonTableExpression<?> limiter = name("limiter")
                                            .fields("real_id","location_code")
                                            .as(
                                                select(field("\"real_id\""),field("\"real_code\""))
                                                .from(data)
                                                .where(field("rownum").lessOrEqual(pageSize))
                                                );
        Field<String> limitId = limiter.field("real_id",String.class);
        Field<Long> limitCode = limiter.field("location_code",Long.class);

        SelectSeekStep3<Record, String, ?, String> query = dsl.with(limiter).select(
                limitId,
                avLoc2.LOCATION_ID.as("alias_id"),
                avLoc2.asterisk())
            .from(limiter)
            .leftOuterJoin(avLoc2).on(avLoc2.LOCATION_CODE.eq(limitCode))
            .orderBy(avLoc2.DB_OFFICE_ID.asc(),limitId.asc(),avLoc2.ALIASED_ITEM.asc());
        logger.log(Level.FINER, () -> query.getSQL(ParamType.INLINED));
        List<? extends CatalogEntry> entries = query
                .fetchSize(1000)
                .fetchStream()
            .map(r -> r.into(AV_LOC2.AV_LOC2))
            .collect(groupingBy(usace.cwms.db.jooq.codegen.tables.records.AV_LOC2::getLOCATION_CODE))
            .values()
            .stream()
            .map(l -> {
                usace.cwms.db.jooq.codegen.tables.records.AV_LOC2 row = l.stream()
                    .filter(r -> r.getALIASED_ITEM() == null)
                    .findFirst()
                    .orElseThrow(() -> new DataAccessException("Could not find location for list of aliases: " + l));
                Set<LocationAlias> aliases = l.stream().filter(r -> r.getALIASED_ITEM() != null)
                    .map(this::buildLocationAlias).collect(toSet());
                return buildCatalogEntry(row, aliases);
            })
            .collect(toList());
        return new Catalog(cursorLocation, total, pageSize, entries);
    }

    private static Condition addCursorConditions(Condition condition, String cursorOffice, String cursorLocation) {
        if (cursorOffice != null) {
            Condition officeEqualCur = AV_LOC2.AV_LOC2.DB_OFFICE_ID.upper().eq(cursorOffice.toUpperCase());
            Condition curOfficeLocationIdGreater = AV_LOC2.AV_LOC2.LOCATION_ID.upper().gt(cursorLocation);
            Condition officeGreaterThanCur = AV_LOC2.AV_LOC2.DB_OFFICE_ID.upper().gt(cursorOffice.toUpperCase());
            condition = condition.and(officeEqualCur).and(curOfficeLocationIdGreater).or(officeGreaterThanCur);
        } else {
            condition = condition.and(AV_LOC2.AV_LOC2.LOCATION_ID.upper().gt(cursorLocation));
        }
        return condition;
    }

    private static Condition buildWhereCondition(String unitSystem, String office, String idLike,
                 String categoryLike, String groupLike, String boundingOfficeLike) {

        Condition condition = caseInsensitiveLikeRegex(AV_LOC2.AV_LOC2.LOCATION_ID, idLike)
                .and(AV_LOC2.AV_LOC2.LOCATION_CODE.notEqual(DELETED_TS_MARKER))
                .and(AV_LOC2.AV_LOC2.UNIT_SYSTEM.equalIgnoreCase(unitSystem));

        if (categoryLike == null && groupLike == null) {
            condition = condition.and(AV_LOC2.AV_LOC2.ALIASED_ITEM.isNull());
        }
        if (office != null) {
            condition = condition.and(AV_LOC2.AV_LOC2.DB_OFFICE_ID.upper().eq(office.toUpperCase()));
        }
        if (categoryLike != null) {
            condition = condition.and(caseInsensitiveLikeRegex(AV_LOC2.AV_LOC2.LOC_ALIAS_CATEGORY, categoryLike));
        }
        if (groupLike != null) {
            condition = condition.and(caseInsensitiveLikeRegex(AV_LOC2.AV_LOC2.LOC_ALIAS_GROUP, groupLike));
        }
        if (boundingOfficeLike != null) {
            condition = condition.and(caseInsensitiveLikeRegex(AV_LOC2.AV_LOC2.BOUNDING_OFFICE_ID, boundingOfficeLike));
        }
        return condition;
    }

    static String warnIfMismatch(String paramName, String pageParam, String queryParam) {
        if (queryParam != null && (!queryParam.equals(pageParam))) {
            logger.log(Level.WARNING, "The {0} query parameter:{1} and page cursor parameter:{2} do not match."
                                    + "  The value provided in the page parameter will be used.",
                            new Object[]{paramName, queryParam, pageParam});
        }
        return pageParam;
    }

    private LocationAlias buildLocationAlias(usace.cwms.db.jooq.codegen.tables.records.AV_LOC2 row) {
        return new LocationAlias(row.getLOC_ALIAS_CATEGORY() + "-" + row.getLOC_ALIAS_GROUP(),
            row.getLOCATION_ID());
    }

    @NotNull
    private static LocationCatalogEntry buildCatalogEntry(usace.cwms.db.jooq.codegen.tables.records.AV_LOC2 loc,
                                                          Set<LocationAlias> aliases) {
        return new LocationCatalogEntry(
            loc.getDB_OFFICE_ID(),
            loc.getLOCATION_ID(),
            loc.getNEAREST_CITY(),
            loc.getPUBLIC_NAME(),
            loc.getLONG_NAME(),
            loc.getDESCRIPTION(),
            loc.getLOCATION_KIND_ID(),
            loc.getLOCATION_TYPE(),
            loc.getTIME_ZONE_NAME(),
            loc.getLATITUDE() != null ? loc.getLATITUDE().doubleValue() : null,
            loc.getLONGITUDE() != null ? loc.getLONGITUDE().doubleValue() : null,
            loc.getPUBLISHED_LATITUDE() != null ? loc.getPUBLISHED_LATITUDE().doubleValue() : null,
            loc.getPUBLISHED_LONGITUDE() != null ? loc.getPUBLISHED_LONGITUDE().doubleValue() : null,
            loc.getHORIZONTAL_DATUM(),
            loc.getELEVATION(),
            loc.getUNIT_ID(),
            loc.getVERTICAL_DATUM(),
            loc.getNATION_ID(),
            loc.getSTATE_INITIAL(),
            loc.getCOUNTY_NAME(),
            loc.getBOUNDING_OFFICE_ID(),
            loc.getMAP_LABEL(),
            loc.getACTIVE_FLAG().equalsIgnoreCase("T"),
            aliases
        );
    }

}
