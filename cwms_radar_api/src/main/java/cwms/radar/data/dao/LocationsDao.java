package cwms.radar.data.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import cwms.radar.data.dto.Location;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.Point;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectConditionStep;

import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;

import static org.jooq.impl.DSL.asterisk;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC.AV_LOC;

public class LocationsDao extends JooqDao<Location> {

    public LocationsDao(DSLContext dsl) {
        super(dsl);
    }


    public String getLocations(String names,String format, String units, String datum, String officeId) {

        return CWMS_LOC_PACKAGE.call_RETRIEVE_LOCATIONS_F(dsl.configuration(),
                names, format, units, datum, officeId);
    }

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
                .map(LocationsDao::buildFeatureFromAvLocRecord)
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

}
