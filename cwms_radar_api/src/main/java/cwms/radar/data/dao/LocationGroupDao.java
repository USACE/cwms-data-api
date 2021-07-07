package cwms.radar.data.dao;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import cwms.radar.data.dto.AssignedLocation;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import kotlin.Pair;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.SelectJoinStep;
import org.jooq.SelectOrderByStep;
import org.jooq.SelectSeekStep1;
import org.jooq.TableField;

import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;

public class LocationGroupDao extends JooqDao<LocationGroup>
{

	public LocationGroupDao(DSLContext dsl)
	{
		super(dsl);
	}

	public LocationGroup getLocationGroup(String officeId, String categoryId, String groupId)
	{
		AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
		AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		final RecordMapper<Record, Pair<LocationGroup, AssignedLocation>> mapper = record17 -> {
			LocationGroup group = buildLocationGroup(record17);
			AssignedLocation loc = buildAssignedLocation(record17);

			return new Pair<>(group, loc);
		};

		List<Pair<LocationGroup, AssignedLocation>> assignments = dsl
				.select(alga.CATEGORY_ID, alga.GROUP_ID,
						alga.LOCATION_CODE, alga.DB_OFFICE_ID, alga.BASE_LOCATION_ID, alga.SUB_LOCATION_ID, alga.LOCATION_ID,
						alga.ALIAS_ID, alga.ATTRIBUTE, alga.REF_LOCATION_ID, alga.SHARED_ALIAS_ID, alga.SHARED_REF_LOCATION_ID,
						alcg.CAT_DB_OFFICE_ID,
						alcg.LOC_CATEGORY_ID, alcg.LOC_CATEGORY_DESC, alcg.LOC_GROUP_DESC, alcg.LOC_GROUP_ATTRIBUTE)
				.from(alcg)
				.join(alga)
				.on(
						alcg.LOC_CATEGORY_ID.eq(alga.CATEGORY_ID)
								.and(
										alcg.LOC_GROUP_ID.eq(alga.GROUP_ID)))
				.where(alcg.LOC_CATEGORY_ID.eq(categoryId).and(alcg.LOC_GROUP_ID.eq(groupId)).and(alga.DB_OFFICE_ID.eq(officeId)))
				.orderBy(alga.ATTRIBUTE)
				.fetch(mapper);

		// Might want to verify that all the groups in the list are the same?
		LocationGroup locGroup = assignments.stream()
				.map(Pair::component1)
				.findFirst().orElse(null);

		if(locGroup != null)
		{
			List<AssignedLocation> assignedLocations = assignments.stream()
					.map(Pair::component2)
					.collect(Collectors.toList());
			locGroup = new LocationGroup(locGroup, assignedLocations);
		}
		return locGroup;
	}

	private AssignedLocation buildAssignedLocation(Record resultRecord)
	{
		AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

		String locationId = resultRecord.get(alga.LOCATION_ID);
		String baseLocationId = resultRecord.get(alga.BASE_LOCATION_ID);
		String subLocationId = resultRecord.get(alga.SUB_LOCATION_ID);
		String aliasId = resultRecord.get(alga.ALIAS_ID);
		Number attribute = resultRecord.get(alga.ATTRIBUTE);
		Number locationCode = resultRecord.get(alga.LOCATION_CODE);
		String refLocationId = resultRecord.get(alga.REF_LOCATION_ID);

		return new AssignedLocation(locationId, baseLocationId, subLocationId, aliasId, attribute,
				locationCode, refLocationId);
	}

	private LocationGroup buildLocationGroup(Record resultRecord)
	{
		// This method needs the record to have fields
		// from both AV_LOC_GRP_ASSGN _and_ AV_LOC_CAT_GRP
		AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
		AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		String officeId = resultRecord.get(alga.DB_OFFICE_ID);
		String groupId = resultRecord.get(alga.GROUP_ID);
		String sharedAliasId = resultRecord.get(alga.SHARED_ALIAS_ID);
		String sharedRefLocationId = resultRecord.get(alga.SHARED_REF_LOCATION_ID);

		String grpDesc = resultRecord.get(alcg.LOC_GROUP_DESC);
		Number grpAttribute = resultRecord.get(alcg.LOC_GROUP_ATTRIBUTE);

		LocationCategory locationCategory = buildLocationCategory(resultRecord);

		return new LocationGroup(
				locationCategory,
				officeId, groupId, grpDesc,
				sharedAliasId, sharedRefLocationId, grpAttribute);
	}

	private LocationCategory buildLocationCategory(Record resultRecord)
	{
		AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		String categoryId = resultRecord.get(alcg.LOC_CATEGORY_ID);
		String catDesc = resultRecord.get(alcg.LOC_CATEGORY_DESC);
		String catDbOfficeId = resultRecord.get(alcg.CAT_DB_OFFICE_ID);
		return new LocationCategory(catDbOfficeId, categoryId, catDesc);
	}

	public List<LocationGroup> getLocationGroups(){
		return getLocationGroups(null);
	}

	public List<LocationGroup> getLocationGroups(String officeId)
	{
		List<LocationGroup> retval;
		AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		org.jooq.TableField [] columns = new TableField[]{
				table.CAT_DB_OFFICE_ID, table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC, table.GRP_DB_OFFICE_ID,
				table.LOC_GROUP_ID, table.LOC_GROUP_DESC, table.SHARED_LOC_ALIAS_ID, table.SHARED_REF_LOCATION_ID,
				table.LOC_GROUP_ATTRIBUTE
		};

		SelectJoinStep<Record> step = dsl.selectDistinct(columns).from(table);

		SelectOrderByStep select = step;

		if(officeId != null && !officeId.isEmpty()){
			select = step.where(
					table.GRP_DB_OFFICE_ID.eq(officeId));
		}

		retval = select.orderBy(table.LOC_GROUP_ATTRIBUTE)
				.fetch().into(LocationGroup.class);

		return retval;
	}

	public Feature buildFeatureFromAvLocRecordWithLocGroup(Record avLocRecord){
		Feature feature = LocationsDao.buildFeatureFromAvLocRecord(avLocRecord);

		Map<String, Object> grpProps = new LinkedHashMap<>();

		AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

		List<Field<?>> fieldsInRecord = Arrays.asList(avLocRecord.fields());

		Set<TableField> grpAssgnFields = new LinkedHashSet<>();
		grpAssgnFields.add(alga.CATEGORY_ID);
		grpAssgnFields.add(alga.GROUP_ID);
		grpAssgnFields.add(alga.ATTRIBUTE);
		grpAssgnFields.add(alga.ALIAS_ID);
		grpAssgnFields.add(alga.SHARED_ALIAS_ID);
		grpAssgnFields.add(alga.SHARED_REF_LOCATION_ID);


		grpAssgnFields.retainAll(fieldsInRecord);

		grpAssgnFields.stream().forEach(f -> grpProps.put(f.getName(), avLocRecord.getValue(f)));

		Map<String, Object> props = feature.getProperties();
		props.put("avLocGrpAssgn", grpProps);
		feature.setProperties(props);
		return feature;
	}


	public FeatureCollection buildFeatureCollectionForLocationGroup(String officeId, String categoryId, String groupId, String units)
	{
		AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
		AV_LOC al = AV_LOC.AV_LOC;

		SelectSeekStep1<Record, BigDecimal> select = dsl.select(al.asterisk(),
				alga.CATEGORY_ID, alga.GROUP_ID,
				alga.ATTRIBUTE, alga.ALIAS_ID, alga.SHARED_REF_LOCATION_ID, alga.SHARED_ALIAS_ID )
				.from(al).join(alga)
				.on(al.LOCATION_ID.eq(alga.LOCATION_ID))
				.where(alga.DB_OFFICE_ID.eq(officeId)
						.and(alga.CATEGORY_ID.eq(categoryId)
								.and(alga.GROUP_ID.eq(groupId))
								.and(al.UNIT_SYSTEM.eq(units))))
				.orderBy(alga.ATTRIBUTE);



		List<Feature> features = select.stream()
				.map(this::buildFeatureFromAvLocRecordWithLocGroup)
				.collect(Collectors.toList());
		FeatureCollection collection = new FeatureCollection();
		collection.setFeatures(features);

		return collection;
	}

}
