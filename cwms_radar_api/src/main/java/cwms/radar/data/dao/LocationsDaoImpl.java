package cwms.radar.data.dao;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC.AV_LOC;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

import cwms.radar.api.NotFoundException;
import cwms.radar.api.enums.Nation;
import cwms.radar.api.enums.Unit;
import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.catalog.CatalogEntry;
import cwms.radar.data.dto.catalog.LocationAlias;
import cwms.radar.data.dto.catalog.LocationCatalogEntry;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.Point;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import usace.cwms.db.dao.ifc.loc.CwmsDbLoc;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.records.AV_LOC;

public class LocationsDaoImpl extends JooqDao<Location> implements LocationsDao {
    private static final Logger logger = Logger.getLogger(LocationsDaoImpl.class.getName());

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
        Location.Builder locationBuilder = new Location.Builder(
                loc.get(AV_LOC.LOCATION_ID),
                loc.get(AV_LOC.LOCATION_KIND_ID),
                zone,
                loc.get(AV_LOC.LATITUDE).doubleValue(),
                loc.get(AV_LOC.LONGITUDE).doubleValue(),
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
    public void deleteLocation(String locationName, String officeId) throws IOException {
        try {
            dsl.connection(c -> {
                CwmsDbLoc locJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLoc.class, c);
                locJooq.delete(c, officeId, locationName);
            });
        } catch (DataAccessException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void storeLocation(Location location) throws IOException {
        location.validate();
        try {
            dsl.connection(c -> {
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
            dsl.connection(c ->            {
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
                .collect(Collectors.toList());
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
                        .map(Map.Entry::getKey).collect(Collectors.toList());
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
    public Catalog getLocationCatalog(String cursor, int pageSize, String unitSystem,
                                      Optional<String> office) {
        return getLocationCatalog(cursor, pageSize, unitSystem, office, null, null, null);
    }

    @Override
    public Catalog getLocationCatalog(String cursor, int pageSize, String unitSystem,
                                      Optional<String> office, String idLike, String categoryLike,
            String groupLike) {
        int total = 0;
        String locCursor = "*";
        if (cursor == null || cursor.isEmpty()) {
            Condition condition = buildCatalogWhere(unitSystem, office, idLike, categoryLike,
                    groupLike);

            SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                    .from(AV_LOC)
                    .innerJoin(AV_LOC_GRP_ASSGN).on(
                            AV_LOC.LOCATION_ID.eq(AV_LOC_GRP_ASSGN.LOCATION_ID))
                    .where(condition);

            total = count.fetchOne().value1();
        } else {
            // get totally from page
            String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "|||");

            if (parts.length > 1) {
                locCursor = parts[0].split("/")[1];
                total = Integer.parseInt(parts[1]);
            }
        }

        Condition condition = buildCatalogWhere(unitSystem, office, idLike, categoryLike, groupLike)
                .and(AV_LOC.LOCATION_ID.upper().greaterThan(locCursor));

        SelectConditionStep<Record1<String>> tmp = dsl.selectDistinct(AV_LOC.LOCATION_ID)
                .from(AV_LOC).innerJoin(AV_LOC_GRP_ASSGN).on(
                        AV_LOC.LOCATION_ID.eq(AV_LOC_GRP_ASSGN.LOCATION_ID))
                .where(condition);

        Table<?> forLimit = tmp.orderBy(AV_LOC.LOCATION_ID).limit(pageSize).asTable();

        SelectConditionStep<Record> query = dsl.select(
                        AV_LOC.asterisk(),
                        AV_LOC_GRP_ASSGN.asterisk()
                )
                .from(AV_LOC)
                .innerJoin(forLimit).on(forLimit.field(AV_LOC.LOCATION_ID).eq(AV_LOC.LOCATION_ID))
                .leftJoin(AV_LOC_GRP_ASSGN).on(AV_LOC_GRP_ASSGN.LOCATION_ID.eq(AV_LOC.LOCATION_ID))
                .where(condition);

        query.orderBy(AV_LOC.LOCATION_ID);
//        logger.info( () -> query.getSQL(ParamType.INLINED));

        Map<usace.cwms.db.jooq.codegen.tables.records.AV_LOC,
                Set<LocationAlias>> theMap = new LinkedHashMap<>();


        query.fetch().forEach(row -> {
            usace.cwms.db.jooq.codegen.tables.records.AV_LOC loc = buildAvLoc(row);

            Set<LocationAlias> locationAliases = theMap.computeIfAbsent(loc,
                    k -> new LinkedHashSet<>());

            locationAliases.add(buildLocationAlias(row));
        });

        List<? extends CatalogEntry> entries =
                theMap.entrySet().stream()
                        .sorted((left, right) -> (
                                left.getKey().getLOCATION_ID()
                                        .compareTo(right.getKey().getBASE_LOCATION_ID())
                        ))
                        .map(e -> buildCatalogEntry(e.getKey(), e.getValue())
                        ).collect(Collectors.toList());

        return new Catalog(locCursor, total, pageSize, entries);
    }

    private LocationAlias buildLocationAlias(Record row) {
       return new LocationAlias(row.getValue(AV_LOC_GRP_ASSGN.CATEGORY_ID)
               + "-" + row.getValue(AV_LOC_GRP_ASSGN.GROUP_ID),
                row.getValue(AV_LOC_GRP_ASSGN.ALIAS_ID));
    }

    @NotNull
    private static LocationCatalogEntry buildCatalogEntry(usace.cwms.db.jooq.codegen.tables.records.AV_LOC loc,
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
                loc.getLATITUDE() != null
                        ? loc.getLATITUDE().doubleValue() : null,
                loc.getLONGITUDE() != null
                        ? loc.getLONGITUDE().doubleValue() : null,
                loc.getPUBLISHED_LATITUDE() != null
                        ? loc.getPUBLISHED_LATITUDE().doubleValue()
                        : null,
                loc.getPUBLISHED_LONGITUDE() != null
                        ? loc.getPUBLISHED_LONGITUDE().doubleValue() : null,
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

    private static usace.cwms.db.jooq.codegen.tables.records.AV_LOC buildAvLoc(Record row) {
        usace.cwms.db.jooq.codegen.tables.records.AV_LOC loc = new AV_LOC();

        loc.setLOCATION_ID(row.getValue(AV_LOC.LOCATION_ID));
        loc.setBASE_LOCATION_ID(row.getValue(AV_LOC.BASE_LOCATION_ID));
        loc.setDB_OFFICE_ID(row.getValue(AV_LOC.DB_OFFICE_ID));
        loc.setNEAREST_CITY(row.getValue(AV_LOC.NEAREST_CITY));
        loc.setPUBLIC_NAME(row.getValue(AV_LOC.PUBLIC_NAME));
        loc.setLONG_NAME(row.getValue(AV_LOC.LONG_NAME));
        loc.setDESCRIPTION(row.getValue(AV_LOC.DESCRIPTION));
        loc.setLOCATION_KIND_ID(row.getValue(AV_LOC.LOCATION_KIND_ID));
        loc.setLOCATION_TYPE(row.getValue(AV_LOC.LOCATION_TYPE));
        loc.setTIME_ZONE_NAME(row.getValue(AV_LOC.TIME_ZONE_NAME));
        loc.setLATITUDE(row.getValue(AV_LOC.LATITUDE));
        loc.setLONGITUDE(row.getValue(AV_LOC.LONGITUDE));
        loc.setPUBLISHED_LATITUDE(row.getValue(AV_LOC.PUBLISHED_LATITUDE));
        loc.setPUBLISHED_LONGITUDE(row.getValue(AV_LOC.PUBLISHED_LONGITUDE));
        loc.setHORIZONTAL_DATUM(row.getValue(AV_LOC.HORIZONTAL_DATUM));
        loc.setELEVATION(row.getValue(AV_LOC.ELEVATION));
        loc.setUNIT_ID(row.getValue(AV_LOC.UNIT_ID));
        loc.setVERTICAL_DATUM(row.getValue(AV_LOC.VERTICAL_DATUM));
        loc.setNATION_ID(row.getValue(AV_LOC.NATION_ID));
        loc.setSTATE_INITIAL(row.getValue(AV_LOC.STATE_INITIAL));
        loc.setCOUNTY_NAME(row.getValue(AV_LOC.COUNTY_NAME));
        loc.setBOUNDING_OFFICE_ID(row.getValue(AV_LOC.BOUNDING_OFFICE_ID));
        loc.setMAP_LABEL(row.getValue(AV_LOC.MAP_LABEL));
        loc.setACTIVE_FLAG(row.getValue(AV_LOC.ACTIVE_FLAG));


        return row.into(AV_LOC);
    }

    private Condition buildCatalogWhere(String unitSystem, Optional<String> office, String idLike) {
        Condition condition = AV_LOC.UNIT_SYSTEM.eq(unitSystem);

        if (idLike != null) {
            condition = condition.and(
                    JooqDao.caseInsensitiveLikeRegex(AV_LOC.LOCATION_ID, idLike));
        }

        if (office.isPresent()) {
            condition = condition.and(
                    JooqDao.caseInsensitiveLikeRegex(AV_LOC.DB_OFFICE_ID,office.get()));
        }
        return condition;
    }

    private Condition buildCatalogWhere(String unitSystem, Optional<String> office,
                                        String idLike, String categoryLike, String groupLike) {
        Condition condition = buildCatalogWhere(unitSystem, office, idLike);

        if (categoryLike != null) {
            condition = condition.and(
                    JooqDao.caseInsensitiveLikeRegex(AV_LOC_GRP_ASSGN.CATEGORY_ID, categoryLike));
        }

        if (groupLike != null) {
            condition = condition.and(
                    JooqDao.caseInsensitiveLikeRegex(AV_LOC_GRP_ASSGN.GROUP_ID,groupLike));
        }

        return condition;
    }

}
