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
import cwms.cda.api.errors.AlreadyExists;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.CwmsIdLocationKind;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.catalog.CatalogEntry;
import cwms.cda.data.dto.catalog.LocationAlias;
import cwms.cda.data.dto.catalog.LocationCatalogEntry;
import cwms.cda.helpers.ZoneIdHelper;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.jooq.OrderField;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.SelectSeekStepN;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import usace.cwms.db.dao.ifc.loc.CwmsDbLoc;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC2;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_OBJ_T;


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

    public List<Location> getLocations(String nameRegex, String unitSystem, String datum, String officeId) {

        Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(AV_LOC.LOCATION_ID, nameRegex);

        if (officeId != null) {
            whereCondition = whereCondition.and(AV_LOC.DB_OFFICE_ID.equalIgnoreCase(officeId));
        }

        if (unitSystem != null) {
            whereCondition = whereCondition.and(AV_LOC.UNIT_SYSTEM.equalIgnoreCase(unitSystem));
        }

        if (datum != null) {
            whereCondition = whereCondition.and(AV_LOC.VERTICAL_DATUM.equalIgnoreCase(datum));
        }

        return dsl.select(AV_LOC.asterisk())
                    .from(AV_LOC)
                    .where(whereCondition)
                    .fetchSize(DEFAULT_SMALL_FETCH_SIZE)
                    .fetch(this::buildLocation);
    }

    @Override
    public List<CwmsIdLocationKind> getLocationKinds(String idRegexMask, String kindRegexMask, String officeId) {
        Condition whereCondition = JooqDao.caseInsensitiveLikeRegexNullTrue(AV_LOC.LOCATION_ID, idRegexMask);

        whereCondition = whereCondition
            .and(JooqDao.caseInsensitiveLikeRegexNullTrue(AV_LOC.LOCATION_KIND_ID, kindRegexMask));

        if (officeId != null) {
            whereCondition = whereCondition.and(AV_LOC.DB_OFFICE_ID.equalIgnoreCase(officeId));
        }

        return dsl.selectDistinct(AV_LOC.LOCATION_ID, AV_LOC.DB_OFFICE_ID, AV_LOC.LOCATION_KIND_ID)
                    .from(AV_LOC)
                    .where(whereCondition)
                    .fetchSize(DEFAULT_SMALL_FETCH_SIZE)
                    .fetch(this::buildLocationKind);
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

    private CwmsIdLocationKind buildLocationKind(Record loc) {
        CwmsIdLocationKind.Builder builder = new CwmsIdLocationKind.Builder();
        builder.withLocationKindId(loc.get(AV_LOC.LOCATION_KIND_ID));
        builder.withLocationId(CwmsId.buildCwmsId(loc.get(AV_LOC.DB_OFFICE_ID), loc.get(AV_LOC.LOCATION_ID)));
        return builder.build();
    }

    private Location buildLocation(Record loc) {
        String timeZoneName = loc.get(AV_LOC.TIME_ZONE_NAME); // may be null...
        ZoneId zone = null;
        if (timeZoneName != null) {
            zone = ZoneIdHelper.parseZoneIdWithAliases(timeZoneName);
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
                .withElevationUnits(loc.get(AV_LOC.UNIT_ID))
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
                .withNation(Nation.nationForName(loc.get(AV_LOC.NATION_ID)))
                ;

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
        connection(dsl, c -> {
            Configuration configuration = getDslContext(c, officeId).configuration();
            if (cascadeDelete) {
                CWMS_LOC_PACKAGE.call_DELETE_LOCATION(configuration, locationName,
                        DELETE_LOC_CASCADE.getRule(), officeId);
            } else {
                CWMS_LOC_PACKAGE.call_DELETE_LOCATION(configuration, locationName,
                        DELETE_LOC.getRule(), officeId);
            }
        });
    }

    /**
     * @deprecated Use {@link #storeLocation(Location, boolean)} instead.
     */
    @Deprecated
    @Override
    public void storeLocation(Location location) throws IOException {
        storeLocation(location, false);
    }

    @Override
    public void storeLocation(Location location, boolean failIfExists) throws IOException {
        location.validate();
        try {
            connection(dsl, c -> {
                setOffice(c, location);
                LOCATION_OBJ_T locationObjT = getLocationObj(location);
                Configuration configuration = getDslContext(c, location.getOfficeId()).configuration();
                try {
                    CWMS_LOC_PACKAGE.call_STORE_LOCATION__2(configuration, locationObjT,
                        formatBool(failIfExists));
                } catch (DataAccessException e) {
                    try {
                        String dbLocationName = CWMS_LOC_PACKAGE.call_GET_LOCATION_ID(
                            configuration, location.getName(), location.getBoundingOfficeId());
                        if (dbLocationName != null && !dbLocationName.isEmpty()) {
                            String message;
                            if (dbLocationName.equalsIgnoreCase(location.getName())) {
                                message = String.format("The location with name: %s already exists in office: %s",
                                    location.getName(), location.getBoundingOfficeId());
                            } else {
                                message = String.format("The location with alias: '%s' and proper name: "
                                        + "'%s' already exists in office: '%s'.",
                                    location.getName(), dbLocationName, location.getBoundingOfficeId());
                            }
                            throw new AlreadyExists(message, e);
                        } else {
                            throw wrapException(e);
                        }
                    } catch (DataAccessException | NotFoundException ignored) {
                        // If we can't find the location by name, then it doesn't exist.
                        // We can throw the original exception.
                        throw wrapException(e);
                    }
                }
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
                String elevationUnits = renamedLocation.getElevationUnits() == null
                        ? Unit.METER.getValue() : renamedLocation.getElevationUnits();
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

        List<Feature> features = selectQuery.fetchSize(DEFAULT_SMALL_FETCH_SIZE).stream()
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



    public Catalog getLocationCatalog(String page, int pageSize, CatalogRequestParameters param) {

        // Parse provided page and pull out the parameters

        Catalog.CatalogPage catPage = null;
        if (page != null && !page.isEmpty()) {
            catPage = new Catalog.CatalogPage(page);

            // The cursor urlencodes the initial query parameters, We should decode them and use the cursor values.
            // If the user provides a page parameter and query parameters they should match.
            // If they don't match its weird and we will log it.
            CatalogRequestParameters.Builder.from(param)
                    .withOffice(warnIfMismatch(OFFICE,
                            catPage.getSearchOffice(), param.getOffice()))
                    .withIdLike(warnIfMismatch(LIKE,
                            catPage.getIdLike(), param.getIdLike()))
                    .withLocCatLike(warnIfMismatch(LOCATION_CATEGORY_LIKE,
                            catPage.getLocCategoryLike(), param.getLocCatLike()))
                    .withLocGroupLike(warnIfMismatch(LOCATION_GROUP_LIKE,
                            catPage.getLocGroupLike(), param.getLocGroupLike()))
                    .withBoundingOfficeLike(warnIfMismatch(BOUNDING_OFFICE_LIKE,
                            catPage.getBoundingOfficeLike(), param.getBoundingOfficeLike()))
                    .build();

        }

        return getLocationCatalog(catPage, pageSize, param);
    }

    private Catalog getLocationCatalog(Catalog.CatalogPage catPage, int pageSize, CatalogRequestParameters params) {
        FieldMapping fieldMapping = null;
        if (params.includeAliases()) {
            fieldMapping = new AvLoc2FieldMapping();
        } else {

            fieldMapping = new AvLocFieldMapping();
        }
        Table<Record> table = fieldMapping.getTable();
        //Now querying against AV_LOC2 as it gives us back the same information as querying against
        //location group views. This makes the code clearer and improves performance.
        //If there is a performance improvement by switching back to location groups and querying against
        //location codes (previous implementation used location_id) for joins, feel free to implement.
        Objects.requireNonNull(params.getIdLike(),
                "A value must be provided for the idLike field. Specify .* if you don't care.");

        // "condition" needs to be used by the count query and the results query.
        Condition condition = buildWhereCondition(params);

        int total;
        String cursorLocation; // The location-id of the cursor in the results
        String cursorOffice; // If the user did not provide a value in the "office" filter then
        // results may contain locations from multiple offices. cursorOffice will track the office
        // of the cursor in the results.
        if (catPage == null) {
            cursorLocation = "*";
            cursorOffice = null;

            SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                .from(table)
                .where(condition);
            logger.log(Level.FINER, () -> count.getSQL(ParamType.INLINED));
            total = count.fetchOne().value1();
        } else {
            cursorLocation = catPage.getCursorId();
            cursorOffice = catPage.getCurOffice();

            total = catPage.getTotal();
            pageSize = catPage.getPageSize();
        }

        condition = addCursorConditions(condition, cursorOffice, cursorLocation, fieldMapping);

        Field<String> dataId = fieldMapping.getLocationId().as("real_id");
        Field<Long> dataCode = fieldMapping.getLocationCode().as("real_code");

        if (fieldMapping.includesAliases()) {
            condition = condition.and(fieldMapping.getAliasedItem().isNull());
        }

        // data/limiter/query
        Table<?> data = dsl.select(dataId,dataCode)
                           .from(table)
                           .where(condition)
                           .orderBy(fieldMapping.getDbOfficeId().asc(), fieldMapping.getLocationId().asc())
                           .asTable("data");
        CommonTableExpression<?> limiter = name("limiter")
                                            .fields("real_id","location_code")
                                            .as(
                                                select(field("\"real_id\""),field("\"real_code\""))
                                                .from(data)
                                                .where(field("rownum").lessOrEqual(pageSize))
                                                );
        Field<String> limitId = limiter.field("real_id",String.class);

        OrderField[] orderFields = new OrderField[2 + (fieldMapping.includesAliases() ? 1 : 0)];
        orderFields[0] = fieldMapping.getDbOfficeId().asc();
        orderFields[1] = limitId.asc();
        if (fieldMapping.includesAliases()) {
            orderFields[2] = fieldMapping.getAliasedItem().asc();
        }

        Field<Long> limitCode = limiter.field("location_code",Long.class);
        SelectSeekStepN<?> query = dsl.with(limiter).select(
                limitId,
                fieldMapping.getLocationId().as("alias_id"),
                table.asterisk())
            .from(limiter)
            .leftOuterJoin(table).on(fieldMapping.getLocationCode().eq(limitCode))
            .orderBy(orderFields);
        logger.log(Level.FINER, () -> query.getSQL(ParamType.INLINED));
        List<? extends CatalogEntry> entries;
        if (fieldMapping.includesAliases()) {
            entries = query
                .fetchSize(DEFAULT_FETCH_SIZE)
                .fetchStream()
                .map(r -> r.into(AV_LOC2.AV_LOC2))
                .collect(groupingBy(usace.cwms.db.jooq.codegen.tables.records.AV_LOC2::getLOCATION_CODE))
                .values()
                .stream()
                .map(l -> {
                    usace.cwms.db.jooq.codegen.tables.records.AV_LOC2 row = l.stream()
                        .filter(r -> r.getALIASED_ITEM() == null)
                        .findFirst()
                        .orElseThrow(
                            () -> new DataAccessException("Could not find location for list of aliases: " + l));
                    Set<LocationAlias> aliases = new HashSet<>();
                    if (params.includeAliases()) {
                        aliases = l.stream().filter(r -> r.getALIASED_ITEM() != null)
                            .map(this::buildLocationAlias).collect(toSet());
                    }
                    return buildCatalogEntry(row, aliases);
                })
                .collect(toList());
        } else {
            entries = query
                .fetchSize(DEFAULT_FETCH_SIZE)
                .fetchStream()
                .map(r -> r.into(AV_LOC))
                .collect(groupingBy(usace.cwms.db.jooq.codegen.tables.records.AV_LOC::getLOCATION_CODE))
                .values()
                .stream()
                .map(l -> {
                    usace.cwms.db.jooq.codegen.tables.records.AV_LOC row = l.stream()
                        .findFirst()
                        .orElseThrow(() -> new DataAccessException("Could not find location: " + l));
                    Set<LocationAlias> aliases = new HashSet<>();
                    return buildCatalogEntry(row, aliases);
                })
                .collect(toList());
        }

        return new Catalog(cursorLocation, total, pageSize, entries, params);
    }

    private static Condition buildWhereCondition(CatalogRequestParameters params) {
        String idLike = params.getIdLike();
        FieldMapping fieldMapping = null;
        if (params.includeAliases()) {
            fieldMapping = new AvLoc2FieldMapping();
        } else {
            fieldMapping = new AvLocFieldMapping();
        }

        Condition condition = caseInsensitiveLikeRegex(fieldMapping.getLocationId(), idLike)
                .and(fieldMapping.getLocationCode().notEqual(DELETED_TS_MARKER))
                .and(fieldMapping.getUnitSystem().equalIgnoreCase(params.getUnitSystem()));

        String groupLike = params.getLocGroupLike();
        String categoryLike = params.getLocCatLike();
        if (categoryLike == null && groupLike == null && params.includeAliases()) {
            condition = condition.and(fieldMapping.getAliasedItem().isNull());
        }

        if (params.includeAliases()) {
            condition =
                condition.and(caseInsensitiveLikeRegexNullTrue(fieldMapping.getGetAliasCategory(), categoryLike));
            condition = condition.and(caseInsensitiveLikeRegexNullTrue(fieldMapping.getGetAliasGroup(), groupLike));
        }

        String office = params.getOffice();
        if (office != null) {
            condition = condition.and(DSL.upper(fieldMapping.getDbOfficeId()).eq(office.toUpperCase()));
        }

        condition = condition.and(caseInsensitiveLikeRegexNullTrue(fieldMapping.getBoundingOfficeId(),
                params.getBoundingOfficeLike()));

        String regexLocationKind = params.getLocationKind();
        if (params.isNegateLocationKindLike() && !regexLocationKind.toUpperCase().startsWith("NOT:")) {
            regexLocationKind = String.format("NOT:%s", regexLocationKind);
        }
        condition = condition.and(caseInsensitiveLikeRegexNullTrue(fieldMapping.getLocationKind(),
            regexLocationKind));

        condition = condition.and(caseInsensitiveLikeRegexNullTrue(fieldMapping.getLocationType(),
                params.getLocationType()));

        if (params.filterBaseLocations() && params.includeAliases()) {
            condition = condition.and(fieldMapping.getSubLocationId().isNotNull());
        }

        return condition;
    }

    private static Condition addCursorConditions(Condition condition, String cursorOffice, String cursorLocation,
                                                 FieldMapping mapping) {
        if (cursorOffice != null) {
            Condition officeEqualCur = DSL.upper(mapping.getDbOfficeId()).eq(cursorOffice.toUpperCase());
            Condition curOfficeLocationIdGreater = DSL.upper(mapping.getLocationId()).gt(cursorLocation);
            Condition officeGreaterThanCur = DSL.upper(mapping.getDbOfficeId()).gt(cursorOffice.toUpperCase());
            condition = condition.and(officeEqualCur).and(curOfficeLocationIdGreater).or(officeGreaterThanCur);
        } else {
            condition = condition.and(DSL.upper(mapping.getLocationId()).gt(cursorLocation));
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

        return new LocationCatalogEntry.Builder()
                .officeId(loc.getDB_OFFICE_ID())
                .name(loc.getLOCATION_ID())
                .nearestCity(loc.getNEAREST_CITY())
                .publicName(loc.getPUBLIC_NAME())
                .longName(loc.getLONG_NAME())
                .description(loc.getDESCRIPTION())
                .kind(loc.getLOCATION_KIND_ID())
                .type(loc.getLOCATION_TYPE())
                .timeZone(loc.getTIME_ZONE_NAME())
                .latitude(loc.getLATITUDE() != null ? loc.getLATITUDE().doubleValue() : null)
                .longitude(loc.getLONGITUDE() != null ? loc.getLONGITUDE().doubleValue() : null)
                .publishedLatitude(loc.getPUBLISHED_LATITUDE() != null
                    ? loc.getPUBLISHED_LATITUDE().doubleValue() : null)
                .publishedLongitude(loc.getPUBLISHED_LONGITUDE() != null
                    ? loc.getPUBLISHED_LONGITUDE().doubleValue() : null)
                .horizontalDatum(loc.getHORIZONTAL_DATUM())
                .elevation(loc.getELEVATION())
                .unit(loc.getUNIT_ID())
                .verticalDatum(loc.getVERTICAL_DATUM())
                .nation(loc.getNATION_ID())
                .state(loc.getSTATE_INITIAL())
                .county(loc.getCOUNTY_NAME())
                .boundingOffice(loc.getBOUNDING_OFFICE_ID())
                .mapLabel(loc.getMAP_LABEL())
                .active(loc.getACTIVE_FLAG().equalsIgnoreCase("T"))
                .aliases(aliases)
                .build();
    }

    @NotNull
    private static LocationCatalogEntry buildCatalogEntry(usace.cwms.db.jooq.codegen.tables.records.AV_LOC loc,
        Set<LocationAlias> aliases) {

        return new LocationCatalogEntry.Builder()
            .officeId(loc.getDB_OFFICE_ID())
            .name(loc.getLOCATION_ID())
            .nearestCity(loc.getNEAREST_CITY())
            .publicName(loc.getPUBLIC_NAME())
            .longName(loc.getLONG_NAME())
            .description(loc.getDESCRIPTION())
            .kind(loc.getLOCATION_KIND_ID())
            .type(loc.getLOCATION_TYPE())
            .timeZone(loc.getTIME_ZONE_NAME())
            .latitude(loc.getLATITUDE() != null ? loc.getLATITUDE().doubleValue() : null)
            .longitude(loc.getLONGITUDE() != null ? loc.getLONGITUDE().doubleValue() : null)
            .publishedLatitude(loc.getPUBLISHED_LATITUDE() != null
                ? loc.getPUBLISHED_LATITUDE().doubleValue() : null)
            .publishedLongitude(loc.getPUBLISHED_LONGITUDE() != null
                ? loc.getPUBLISHED_LONGITUDE().doubleValue() : null)
            .horizontalDatum(loc.getHORIZONTAL_DATUM())
            .elevation(loc.getELEVATION())
            .unit(loc.getUNIT_ID())
            .verticalDatum(loc.getVERTICAL_DATUM())
            .nation(loc.getNATION_ID())
            .state(loc.getSTATE_INITIAL())
            .county(loc.getCOUNTY_NAME())
            .boundingOffice(loc.getBOUNDING_OFFICE_ID())
            .mapLabel(loc.getMAP_LABEL())
            .active(loc.getACTIVE_FLAG().equalsIgnoreCase("T"))
            .aliases(aliases)
            .build();
    }

    private static LOCATION_OBJ_T getLocationObj(Location location) {
        LOCATION_OBJ_T retval = null;
        if (location != null) {
            retval = new LOCATION_OBJ_T();
            String elevationUnits = location.getElevationUnits() == null
                ? Unit.METER.getValue() : location.getElevationUnits();
            retval.setLOCATION_REF(LocationUtil.getLocationRef(location.getName(), location.getOfficeId()));
            retval.setLOCATION_KIND_ID(location.getLocationKind());
            retval.setTIME_ZONE_NAME(location.getTimezoneName());
            retval.setLATITUDE(toBigDecimal(location.getLatitude()));
            retval.setLONGITUDE(toBigDecimal(location.getLongitude()));
            retval.setHORIZONTAL_DATUM(location.getHorizontalDatum());
            retval.setACTIVE_FLAG(formatBool(location.getActive()));
            retval.setDESCRIPTION(location.getDescription());
            retval.setELEVATION(toBigDecimal(location.getElevation()));
            retval.setELEV_UNIT_ID(elevationUnits);
            retval.setCOUNTY_NAME(location.getCountyName());
            retval.setBOUNDING_OFFICE_ID(location.getBoundingOfficeId());
            retval.setNATION_ID(Optional.ofNullable(location.getNation()).map(Nation::getName).orElse(null));
            retval.setMAP_LABEL(location.getMapLabel());
            retval.setPUBLIC_NAME(location.getPublicName());
            retval.setPUBLISHED_LATITUDE(toBigDecimal(location.getPublishedLatitude()));
            retval.setPUBLISHED_LONGITUDE(toBigDecimal(location.getPublishedLongitude()));
            retval.setVERTICAL_DATUM(location.getVerticalDatum());
            retval.setLONG_NAME(location.getLongName());
            retval.setSTATE_INITIAL(location.getStateInitial());
            retval.setLOCATION_TYPE(location.getLocationType());
            retval.setNEAREST_CITY(location.getNearestCity());
        }
        return retval;
    }

    private interface FieldMapping {
        Field<Long> getLocationCode();

        Field<String> getLocationId();

        Field<String> getAliasedItem();

        Field<String> getDbOfficeId();

        Field<String> getUnitSystem();

        Field<String> getLocationType();

        Field<String> getLocationKind();

        Field<String> getBoundingOfficeId();

        Field<String> getGetAliasGroup();

        Field<String> getGetAliasCategory();

        Field<String> getSubLocationId();

        boolean includesAliases();

        Table<Record> getTable();
    }

    private static class AvLoc2FieldMapping implements FieldMapping {
        @Override
        public Field<Long> getLocationCode() {
            return AV_LOC2.AV_LOC2.LOCATION_CODE;
        }

        @Override
        public Field<String> getLocationId() {
            return AV_LOC2.AV_LOC2.LOCATION_ID;
        }

        @Override
        public Field<String> getAliasedItem() {
            return AV_LOC2.AV_LOC2.ALIASED_ITEM;
        }

        @Override
        public Field<String> getDbOfficeId() {
            return AV_LOC2.AV_LOC2.DB_OFFICE_ID;
        }

        @Override
        public Field<String> getUnitSystem() {
            return AV_LOC2.AV_LOC2.UNIT_SYSTEM;
        }

        @Override
        public Field<String> getLocationType() {
            return AV_LOC2.AV_LOC2.LOCATION_TYPE;
        }

        @Override
        public Field<String> getLocationKind() {
            return AV_LOC2.AV_LOC2.LOCATION_KIND_ID;
        }

        @Override
        public Field<String> getBoundingOfficeId() {
            return AV_LOC2.AV_LOC2.BOUNDING_OFFICE_ID;
        }

        @Override
        public Field<String> getGetAliasGroup() {
            return AV_LOC2.AV_LOC2.LOC_ALIAS_GROUP;
        }

        @Override
        public Field<String> getGetAliasCategory() {
            return AV_LOC2.AV_LOC2.LOC_ALIAS_CATEGORY;
        }

        @Override
        public Field<String> getSubLocationId() {
            return AV_LOC2.AV_LOC2.SUB_LOCATION_ID;
        }

        @Override
        public boolean includesAliases() {
            return true;
        }

        @Override
        public TableImpl getTable() {
            return AV_LOC2.AV_LOC2;
        }
    }

    private static class AvLocFieldMapping implements FieldMapping {
        @Override
        public Field<Long> getLocationCode() {
            return AV_LOC.LOCATION_CODE;
        }

        @Override
        public Field<String> getLocationId() {
            return AV_LOC.LOCATION_ID;
        }

        @Override
        public Field<String> getAliasedItem() {
            return null;
        }

        @Override
        public Field<String> getDbOfficeId() {
            return AV_LOC.DB_OFFICE_ID;
        }

        @Override
        public Field<String> getUnitSystem() {
            return AV_LOC.UNIT_SYSTEM;
        }

        @Override
        public Field<String> getLocationType() {
            return AV_LOC.LOCATION_TYPE;
        }

        @Override
        public Field<String> getLocationKind() {
            return AV_LOC.LOCATION_KIND_ID;
        }

        @Override
        public Field<String> getBoundingOfficeId() {
            return AV_LOC.BOUNDING_OFFICE_ID;
        }

        @Override
        public Field<String> getGetAliasGroup() {
            return null;
        }

        @Override
        public Field<String> getGetAliasCategory() {
            return null;
        }

        @Override
        public Field<String> getSubLocationId() {
            return null;
        }

        @Override
        public boolean includesAliases() {
            return false;
        }

        @Override
        public TableImpl getTable() {
            return AV_LOC;
        }
    }
}
