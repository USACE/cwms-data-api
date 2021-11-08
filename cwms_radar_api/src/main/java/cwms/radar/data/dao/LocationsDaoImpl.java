package cwms.radar.data.dao;

import java.io.IOException;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.api.enums.Nation;
import cwms.radar.api.enums.Unit;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.catalog.CatalogEntry;
import cwms.radar.data.dto.catalog.LocationAlias;
import cwms.radar.data.dto.catalog.LocationCatalogEntry;

import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.Point;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.Table;

import org.jooq.exception.DataAccessException;
import usace.cwms.db.dao.ifc.loc.CwmsDbLoc;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;

import static org.jooq.impl.DSL.*;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC.AV_LOC;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC_ALIAS.AV_LOC_ALIAS;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

public class LocationsDaoImpl extends JooqDao<Location> implements LocationsDao
{
    private static final Logger logger = Logger.getLogger(LocationsDaoImpl.class.getName());

    public LocationsDaoImpl(DSLContext dsl) {
        super(dsl);
    }


    @Override
    public String getLocations(String names,String format, String units, String datum, String officeId) {

        return CWMS_LOC_PACKAGE.call_RETRIEVE_LOCATIONS_F(dsl.configuration(),
                names, format, units, datum, officeId);
    }

    @Override
    public Location getLocation(String locationName, String unitSystem, String officeId) throws IOException
    {
        String[] locationId = new String[]{locationName};
        String elevationUnitId = (unitSystem.equals(UnitSystem.EN.getValue())) ? "ft" : "m";
        String[] locationType = new String[1];
        Number[] elevation = new Number[1];
        String[] verticalDatum = new String[1];
        Number[] latitude = new Number[1];
        Number[] longitude = new Number[1];
        String[] horizontalDatum = new String[1];
        String[] publicName = new String[1];
        String[] longName = new String[1];
        String[] description = new String[1];
        String[] timezoneId = new String[1];
        String[] countyName = new String[1];
        String[] stateInitial = new String[1];
        Boolean[] active = new Boolean[1];
        String[] locationKindId = new String[1];
        String[] mapLabel = new String[1];
        Number[] publishedLatitude = new Number[1];
        Number[] publishedLongitude = new Number[1];
        String[] boundingOfficeId = new String[1];
        String[] nationId = new String[1];
        String[] nearestCity = new String[1];
        ResultSet[] aliasResultSet = new ResultSet[1];
        AtomicReference<Location> locationRef = new AtomicReference<>();
        try {
            dsl.connection(c ->
            {
                CwmsDbLoc locJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLoc.class, c);
                locJooq.retrieve2(c, officeId, locationId, elevationUnitId, locationType, elevation, verticalDatum, latitude, longitude,
                        horizontalDatum, publicName, longName, description, timezoneId, countyName, stateInitial, active, locationKindId,
                        mapLabel, publishedLatitude, publishedLongitude, boundingOfficeId, nationId, nearestCity, aliasResultSet);
                Location location = new Location.Builder(locationId[0], locationKindId[0], ZoneId.of(timezoneId[0]), latitude[0].doubleValue(), longitude[0].doubleValue(), horizontalDatum[0], officeId)
                        .withLocationType(locationType[0])
                        .withElevation(elevation[0].doubleValue())
                        .withVerticalDatum(verticalDatum[0])
                        .withPublicName(publicName[0])
                        .withLongName(longName[0])
                        .withDescription(description[0])
                        .withCountyName(countyName[0])
                        .withStateInitial(stateInitial[0])
                        .withActive(active[0])
                        .withMapLabel(mapLabel[0])
                        .withBoundingOfficeId(boundingOfficeId[0])
                        .withNearestCity(nearestCity[0])
                        .withNation(Nation.NationForName(nationId[0]))
                        .build();
                if (publishedLatitude[0] != null) {
                    location = new Location.Builder(location)
                            .withPublishedLatitude(publishedLatitude[0].doubleValue())
                            .build();
                }
                if (publishedLongitude[0] != null) {
                    location = new Location.Builder(location)
                            .withPublishedLongitude(publishedLongitude[0].doubleValue())
                            .build();
                }
                locationRef.set(location);
            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException("Could not retrieve Location");
        }
        return locationRef.get();
    }

    @Override
    public void deleteLocation(String locationName, String officeId) throws IOException
    {
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLoc locJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLoc.class, c);
                locJooq.delete(c, officeId, locationName);
            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException(ex);
        }
    }

    @Override
    public void storeLocation(Location location) throws IOException
    {
        validateLocation(location);
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLoc locJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLoc.class, c);
                String elevationUnits = Unit.METER.getValue();
                locJooq.store(c, location.getOfficeId(), location.getName(), location.getStateInitial(), location.getCountyName(),
                        location.getTimezoneName(), location.getLocationType(), location.getLatitude(), location.getLongitude(), location.getElevation(),
                        elevationUnits, location.getVerticalDatum(), location.getHorizontalDatum(), location.getPublicName(), location.getLongName(),
                        location.getDescription(), location.active(), location.getLocationKind(), location.getMapLabel(), location.getPublishedLatitude(),
                        location.getPublishedLongitude(), location.getBoundingOfficeId(), location.getNation().getName(), location.getNearestCity(), true);

            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException("Failed to store Location");
        }
    }

    private void validateLocation(Location location) throws IOException
    {
        String missingField = null;
        if(location.getName() == null)
        {
            missingField = "Name";
        }
        if(location.getLocationKind() == null)
        {
            missingField = "Location Kind";
        }
        if(location.getTimezoneName() == null)
        {
            missingField = "Timezone ID";
        }
        if(location.getOfficeId() == null)
        {
            missingField = "Office ID";
        }
        if(location.getHorizontalDatum() == null)
        {
            missingField = "Horizontal Datum";
        }
        if(location.getLongitude() == null)
        {
            missingField = "Longitude";
        }
        if(location.getLatitude() == null)
        {
            missingField = "Latitude";
        }
        if(missingField != null)
        {
            throw new IOException("Missing required field: " + missingField);
        }
    }

    @Override
    public void renameLocation(String oldLocationName, Location renamedLocation) throws IOException
    {
        validateLocation(renamedLocation);
        try
        {
            dsl.connection(c ->
            {
                CwmsDbLoc locJooq = CwmsDbServiceLookup.buildCwmsDb(CwmsDbLoc.class, c);
                String elevationUnits = Unit.METER.getValue();
                locJooq.rename(c, renamedLocation.getOfficeId(), oldLocationName, renamedLocation.getName(), renamedLocation.getStateInitial(),
                        renamedLocation.getCountyName(), renamedLocation.getTimezoneName(), renamedLocation.getLocationType(),
                        renamedLocation.getLatitude(), renamedLocation.getLongitude(), renamedLocation.getElevation(), elevationUnits,
                        renamedLocation.getVerticalDatum(), renamedLocation.getHorizontalDatum(), renamedLocation.getPublicName(),
                        renamedLocation.getLongName(), renamedLocation.getDescription(), renamedLocation.active(),  true);
            });
        }
        catch(DataAccessException ex)
        {
            throw new IOException("Failed to rename Location");
        }
    }

    @Override
    public FeatureCollection buildFeatureCollection(String names, String units, String officeId)
    {
        if(!"EN".equals(units)){
            units = "SI";
        }

        SelectConditionStep<Record> selectQuery = dsl.select(asterisk())
                .from(AV_LOC)
                .where(AV_LOC.DB_OFFICE_ID.eq(officeId))
                .and(AV_LOC.UNIT_SYSTEM.eq(units))
                ;

        if(names != null && !names.isEmpty()){
            List<String> identifiers = new ArrayList<>();
            if(names.contains("|")){
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

    public static Feature buildFeatureFromAvLocRecord( Record avLocRecord)
    {
        Feature feature = new Feature();

        String featureId = avLocRecord.getValue(AV_LOC.PUBLIC_NAME, String.class);
        if(featureId == null || featureId.isEmpty()){
            featureId = avLocRecord.getValue(AV_LOC.LOCATION_ID, String.class);
        }
        feature.setId(featureId);

        Double longitude = avLocRecord.getValue(AV_LOC.LONGITUDE, Double.class);
        Double latitude = avLocRecord.getValue(AV_LOC.LATITUDE, Double.class);

        if(latitude == null)
        {
            latitude = 0.0;
        }

        if(longitude == null){
            longitude = 0.0;
        }

        feature.setGeometry(new Point(longitude, latitude));

        Map<String, Object> properties = new LinkedHashMap<>();
        Map<String, Object> recordMap = avLocRecord.intoMap();
        List<String> keysWithNullValue =
                recordMap.entrySet().stream().filter(e -> e.getValue() == null)
                        .map(Map.Entry::getKey).collect(Collectors.toList());
        keysWithNullValue.forEach(recordMap::remove);
        recordMap.remove(AV_LOC.LATITUDE.getName());
        recordMap.remove(AV_LOC.LONGITUDE.getName());
        recordMap.remove(AV_LOC.PUBLIC_NAME.getName());
        properties.put("avLoc", recordMap);
        feature.setProperties(properties);

        return feature;
    }

    @Override
    public Catalog getLocationCatalog(String cursor, int pageSize, String unitSystem, Optional<String> office) {
        int total = 0;
        String locCursor = "*";
        if( cursor == null || cursor.isEmpty() ){
            SelectJoinStep<Record1<Integer>> count = dsl.select(count(asterisk())).from(AV_LOC);
            if( office.isPresent() ){
                count.where(AV_LOC.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()));
            }
            total = count.fetchOne().value1().intValue();
        } else {
            logger.info("getting non-default page");
            // get totally from page
            String[] parts = Catalog.decodeCursor(cursor, "|||");

            logger.info("decoded cursor: " + String.join("|||", parts));
            for( String p: parts){
                logger.info(p);
            }

            if(parts.length > 1) {
                locCursor = parts[0].split("\\/")[1];
                total = Integer.parseInt(parts[1]);
            }
        }

        SelectConditionStep<Record1<String>> tmp = dsl.select(AV_LOC.LOCATION_ID)
                               .from(AV_LOC)
                               .where(AV_LOC.LOCATION_ID.greaterThan(locCursor))
                               .and(AV_LOC.UNIT_SYSTEM.eq(unitSystem));
        if( office.isPresent()){
            tmp = tmp.and(AV_LOC.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()));
        }
        Table<?> forLimit = tmp.orderBy(AV_LOC.BASE_LOCATION_ID).limit(pageSize).asTable();
        SelectConditionStep<Record> query = dsl.select(
                                    AV_LOC.asterisk(),
                                    AV_LOC_GRP_ASSGN.asterisk()
                                )
                                .from(AV_LOC)
                                .innerJoin(forLimit).on(forLimit.field(AV_LOC.LOCATION_ID).eq(AV_LOC.LOCATION_ID))
                                .leftJoin(AV_LOC_GRP_ASSGN).on(AV_LOC_GRP_ASSGN.LOCATION_ID.eq(AV_LOC.LOCATION_ID))
                                .where(AV_LOC.UNIT_SYSTEM.eq(unitSystem))
                                .and(AV_LOC.LOCATION_ID.upper().greaterThan(locCursor));

        if( office.isPresent() ){
            query.and(AV_LOC.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()));
        }
        query.orderBy(AV_LOC.LOCATION_ID);
        HashMap<usace.cwms.db.jooq.codegen.tables.records.AV_LOC, ArrayList<usace.cwms.db.jooq.codegen.tables.records.AV_LOC_ALIAS>> theMap = new HashMap<>();
        //Result<?> result = query.fetch();
        query.fetch().forEach( row -> {
            usace.cwms.db.jooq.codegen.tables.records.AV_LOC loc = row.into(AV_LOC);
            if( !theMap.containsKey(loc)){
                theMap.put(loc, new ArrayList<>() );
            }
            usace.cwms.db.jooq.codegen.tables.records.AV_LOC_ALIAS alias = row.into(AV_LOC_ALIAS);
            usace.cwms.db.jooq.codegen.tables.records.AV_LOC_GRP_ASSGN group = row.into(AV_LOC_GRP_ASSGN);
            if( group.getALIAS_ID() != null ){
                theMap.get(loc).add(alias);
            }
        });

        List<? extends CatalogEntry> entries =
        theMap.entrySet().stream().map( e -> {
            LocationCatalogEntry ce = new LocationCatalogEntry(
                e.getKey().getDB_OFFICE_ID(),
                e.getKey().getLOCATION_ID(),
                e.getKey().getNEAREST_CITY(),
                e.getKey().getPUBLIC_NAME(),
                e.getKey().getLONG_NAME(),
                e.getKey().getDESCRIPTION(),
                e.getKey().getLOCATION_KIND_ID(),
                e.getKey().getLOCATION_TYPE(),
                e.getKey().getTIME_ZONE_NAME(),
                e.getKey().getLATITUDE() != null ? e.getKey().getLATITUDE().doubleValue() : null,
                e.getKey().getLONGITUDE() != null ? e.getKey().getLONGITUDE().doubleValue() : null,
                e.getKey().getPUBLISHED_LATITUDE() != null ? e.getKey().getPUBLISHED_LATITUDE().doubleValue() : null,
                e.getKey().getPUBLISHED_LONGITUDE() != null ? e.getKey().getPUBLISHED_LONGITUDE().doubleValue() : null,
                e.getKey().getHORIZONTAL_DATUM(),
                e.getKey().getELEVATION(),
                e.getKey().getUNIT_ID(),
                e.getKey().getVERTICAL_DATUM(),
                e.getKey().getNATION_ID(),
                e.getKey().getSTATE_INITIAL(),
                e.getKey().getCOUNTY_NAME(),
                e.getKey().getBOUNDING_OFFICE_ID(),
                e.getKey().getMAP_LABEL(),
                e.getKey().getACTIVE_FLAG().equalsIgnoreCase("T") ? true : false,
                e.getValue().stream().map( a -> {
                    return new LocationAlias(a.getCATEGORY_ID()+"-"+a.getGROUP_ID(),a.getALIAS_ID());
                }).collect(Collectors.toList())
            );

            return ce;
        }).collect(Collectors.toList());

        Catalog cat = new Catalog(locCursor,total,pageSize,entries);
        return cat;
    }


}
