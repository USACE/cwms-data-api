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

package cwms.radar.data.dao;

import static java.util.stream.Collectors.toList;

import cwms.radar.data.dto.AssignedLocation;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import kotlin.Pair;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.SelectConnectByStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectOnConditionStep;
import org.jooq.SelectOrderByStep;
import org.jooq.SelectSeekStep1;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.udt.records.LOC_ALIAS_ARRAY3;
import usace.cwms.db.jooq.codegen.udt.records.LOC_ALIAS_TYPE3;

public final class LocationGroupDao extends JooqDao<LocationGroup> {

    public static final String CWMS = "CWMS";

    public LocationGroupDao(DSLContext dsl) {
        super(dsl);
    }

    public Optional<LocationGroup> getLocationGroup(String officeId, String categoryId,
                                                    String groupId) {
        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
        AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        final RecordMapper<Record, Pair<LocationGroup, AssignedLocation>> mapper = grpRecord -> {
            LocationCategory locationCategory = buildLocationCategory(grpRecord);
            LocationGroup group = buildLocationGroup(grpRecord, locationCategory);
            AssignedLocation loc = buildAssignedLocation(grpRecord);

            return new Pair<>(group, loc);
        };

        Condition assignmentOffice;
        if (CWMS.equalsIgnoreCase(officeId)) {
            assignmentOffice = DSL.trueCondition();
        } else {
            assignmentOffice = alga.DB_OFFICE_ID.isNull().or(alga.DB_OFFICE_ID.eq(officeId));
        }

        List<Pair<LocationGroup, AssignedLocation>> assignments = dsl.select(
                alcg.CAT_DB_OFFICE_ID,
                alcg.LOC_CATEGORY_ID,
                alcg.LOC_CATEGORY_DESC,
                alcg.GRP_DB_OFFICE_ID,
                alcg.LOC_GROUP_ID,
                alcg.LOC_GROUP_DESC,
                alcg.LOC_GROUP_ATTRIBUTE,
                alcg.SHARED_LOC_ALIAS_ID,
                alcg.SHARED_REF_LOCATION_ID,
                alga.DB_OFFICE_ID,
                alga.LOCATION_ID,
                alga.ALIAS_ID,
                alga.ATTRIBUTE,
                alga.REF_LOCATION_ID)
            .from(alcg).leftJoin(alga)
            .on(alcg.LOC_CATEGORY_ID.eq(alga.CATEGORY_ID)
                .and(alcg.LOC_GROUP_ID.eq(alga.GROUP_ID)))
            .where(alcg.LOC_CATEGORY_ID.eq(categoryId)
                .and(alcg.LOC_GROUP_ID.eq(groupId))
                .and(alcg.GRP_DB_OFFICE_ID.in(CWMS, officeId))
                .and(alcg.CAT_DB_OFFICE_ID.in(CWMS, officeId))
                .and(assignmentOffice)
            )
            .orderBy(alga.ATTRIBUTE).fetchSize(1000).fetch(mapper);

        // Might want to verify that all the groups in the list are the same?
        LocationGroup locGroup =
                assignments.stream().map(Pair::component1).findFirst().orElse(null);

        if (locGroup != null) {
            List<AssignedLocation> assignedLocations = assignments.stream()
                .map(Pair::component2)
                .filter(Objects::nonNull)
                .collect(toList());
            locGroup = new LocationGroup(locGroup, assignedLocations);
        }
        return Optional.ofNullable(locGroup);
    }

    private AssignedLocation buildAssignedLocation(Record resultRecord) {
        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

        String locationId = resultRecord.get(alga.LOCATION_ID);
        String officeId = resultRecord.get(alga.DB_OFFICE_ID);

        String aliasId = resultRecord.get(alga.ALIAS_ID);
        Number attribute = resultRecord.get(alga.ATTRIBUTE);

        String refLocationId = resultRecord.get(alga.REF_LOCATION_ID);

        if(locationId == null) {
            return null;
        }
        return new AssignedLocation(locationId, officeId, aliasId, attribute, refLocationId);
    }

    private LocationGroup buildLocationGroup(Record resultRecord,
                                             LocationCategory locationCategory) {
        AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        String groupId = resultRecord.get(alcg.LOC_GROUP_ID);
        String sharedAliasId = resultRecord.get(alcg.SHARED_LOC_ALIAS_ID);
        String sharedRefLocationId = resultRecord.get(alcg.SHARED_REF_LOCATION_ID);

        String grpOfficeId = resultRecord.get(alcg.GRP_DB_OFFICE_ID);
        String grpDesc = resultRecord.get(alcg.LOC_GROUP_DESC);
        Number grpAttribute = resultRecord.get(alcg.LOC_GROUP_ATTRIBUTE);

        return new LocationGroup(locationCategory, grpOfficeId, groupId, grpDesc, sharedAliasId,
                sharedRefLocationId, grpAttribute);
    }

    private LocationCategory buildLocationCategory(Record resultRecord) {
        AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        String catDbOfficeId = resultRecord.get(alcg.CAT_DB_OFFICE_ID);
        String categoryId = resultRecord.get(alcg.LOC_CATEGORY_ID);
        String catDesc = resultRecord.get(alcg.LOC_CATEGORY_DESC);
        return new LocationCategory(catDbOfficeId, categoryId, catDesc);
    }

    public List<LocationGroup> getLocationGroups() {
        return getLocationGroups(null);
    }

    public List<LocationGroup> getLocationGroups(String officeId, boolean includeAssigned) {
        if (includeAssigned) {
            return getLocationGroups(officeId);
        } else {
            return getGroupsWithoutAssignedLocations(officeId);
        }
    }

    public List<LocationGroup> getLocationGroups(String officeId) {
        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
        AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        final RecordMapper<Record, Pair<LocationGroup, AssignedLocation>> mapper = grpRecord -> {
            LocationCategory category = buildLocationCategory(grpRecord);

            LocationGroup group = buildLocationGroup(grpRecord, category);
            AssignedLocation loc = buildAssignedLocation(grpRecord);

            return new Pair<>(group, loc);
        };

        Map<LocationGroup, List<AssignedLocation>> map = new LinkedHashMap<>();

        SelectConnectByStep<? extends Record> connectBy;
        SelectOnConditionStep<? extends Record> onStep = dsl.select(
                        alcg.CAT_DB_OFFICE_ID,
                        alcg.LOC_CATEGORY_ID,
                        alcg.LOC_CATEGORY_DESC,
                        alcg.GRP_DB_OFFICE_ID,
                        alcg.LOC_GROUP_ID,
                        alcg.LOC_GROUP_DESC,
                        alcg.LOC_GROUP_ATTRIBUTE,
                        alcg.SHARED_LOC_ALIAS_ID,
                        alcg.SHARED_REF_LOCATION_ID,
                        alga.DB_OFFICE_ID,
                        alga.LOCATION_ID,
                        alga.ALIAS_ID,
                        alga.ATTRIBUTE,
                        alga.REF_LOCATION_ID)
                .from(alcg).leftJoin(alga)
                            .on(alcg.LOC_CATEGORY_ID.eq(alga.CATEGORY_ID)
                            .and(alcg.LOC_GROUP_ID.eq(alga.GROUP_ID)));
        if (officeId != null) {
            if (CWMS.equalsIgnoreCase(officeId)) {
                connectBy = onStep.where(alcg.CAT_DB_OFFICE_ID.eq(CWMS)
                        .and(alcg.GRP_DB_OFFICE_ID.eq(CWMS)));
            } else {
                connectBy = onStep.where(alcg.CAT_DB_OFFICE_ID.in(CWMS, officeId)
                        .and(alcg.GRP_DB_OFFICE_ID.in(CWMS, officeId))
                        .and(alga.DB_OFFICE_ID.isNull().or(alga.DB_OFFICE_ID.eq(officeId))));
            }
        } else {
            connectBy = onStep.where(alcg.LOC_GROUP_ID.isNotNull());
        }

        connectBy.orderBy(alcg.LOC_CATEGORY_ID, alcg.LOC_GROUP_ID, alga.ATTRIBUTE).fetchSize(1000)    //
                // This made the query go from 2 minutes to 10 seconds?
                .stream().map(mapper::map).forEach(pair -> {
                    LocationGroup locationGroup = pair.component1();
                    List<AssignedLocation> list = map.computeIfAbsent(locationGroup, k -> new ArrayList<>());
                    AssignedLocation assignedLocation = pair.component2();
                    if(assignedLocation != null) {
                        list.add(assignedLocation);
                    }
                });

        List<LocationGroup> retval = new ArrayList<>();
        for (final Map.Entry<LocationGroup, List<AssignedLocation>> entry : map.entrySet()) {
            retval.add(new LocationGroup(entry.getKey(), entry.getValue()));
        }
        return retval;
    }

    private List<LocationGroup> getGroupsWithoutAssignedLocations(String officeId) {
        List<LocationGroup> retval;
        AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        TableField[] columns = new TableField[]{table.CAT_DB_OFFICE_ID, table.LOC_CATEGORY_ID,
                table.LOC_CATEGORY_DESC, table.GRP_DB_OFFICE_ID, table.LOC_GROUP_ID,
                table.LOC_GROUP_DESC, table.SHARED_LOC_ALIAS_ID, table.SHARED_REF_LOCATION_ID,
                table.LOC_GROUP_ATTRIBUTE};

        SelectJoinStep<Record> step = dsl.selectDistinct(columns).from(table);

        SelectOrderByStep<?> select = step;

        if (officeId != null && !officeId.isEmpty()) {
            select = step.where(table.GRP_DB_OFFICE_ID.eq(officeId));
        }

        retval = select.orderBy(table.LOC_CATEGORY_ID, table.LOC_GROUP_ATTRIBUTE,
                table.LOC_GROUP_ID).fetchSize(1000).fetch().into(LocationGroup.class);

        return retval;
    }

    public Feature buildFeatureFromAvLocRecordWithLocGroup(Record avLocRecord) {

        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

        List<Field<?>> fieldsInRecord = Arrays.asList(avLocRecord.fields());

        Set<TableField<?, ?>> grpAssgnFields = new LinkedHashSet<>();
        grpAssgnFields.add(alga.CATEGORY_ID);
        grpAssgnFields.add(alga.GROUP_ID);
        grpAssgnFields.add(alga.ATTRIBUTE);
        grpAssgnFields.add(alga.ALIAS_ID);
        grpAssgnFields.add(alga.SHARED_ALIAS_ID);
        grpAssgnFields.add(alga.SHARED_REF_LOCATION_ID);

        grpAssgnFields.retainAll(fieldsInRecord);

        Map<String, Object> grpProps = new LinkedHashMap<>();
        grpAssgnFields.stream().forEach(f -> grpProps.put(f.getName(), avLocRecord.getValue(f)));

        Feature feature = LocationsDaoImpl.buildFeatureFromAvLocRecord(avLocRecord);
        Map<String, Object> props = feature.getProperties();
        props.put("avLocGrpAssgn", grpProps);
        feature.setProperties(props);
        return feature;
    }

    public FeatureCollection buildFeatureCollectionForLocationGroup(String officeId,
                                                                    String categoryId,
                                                                    String groupId, String units) {
        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
        AV_LOC al = AV_LOC.AV_LOC;

        SelectSeekStep1<Record, BigDecimal> select = dsl.select(al.asterisk(), alga.CATEGORY_ID,
                        alga.GROUP_ID, alga.ATTRIBUTE, alga.ALIAS_ID, alga.SHARED_REF_LOCATION_ID,
                        alga.SHARED_ALIAS_ID)
                .from(al).join(alga).on(al.LOCATION_ID.eq(alga.LOCATION_ID))
                .where(alga.DB_OFFICE_ID.eq(officeId)
                        .and(alga.CATEGORY_ID.eq(categoryId)
                                .and(alga.GROUP_ID.eq(groupId))
                                .and(al.UNIT_SYSTEM.eq(units))))
                .orderBy(alga.ATTRIBUTE);

        List<Feature> features =
                select.stream()
                        .map(this::buildFeatureFromAvLocRecordWithLocGroup)
                        .collect(toList());
        FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(features);

        return collection;
    }

    public void delete(String categoryId, String groupId, boolean cascadeDelete, String office) {
        CWMS_LOC_PACKAGE.call_DELETE_LOC_GROUP__2(dsl.configuration(), categoryId, groupId,
            OracleTypeMap.formatBool(cascadeDelete), office);
    }

    public void create(LocationGroup group) {
        Configuration configuration = dsl.configuration();
        String categoryId = group.getLocationCategory().getId();
        CWMS_LOC_PACKAGE.call_CREATE_LOC_GROUP2(configuration, categoryId,
            group.getId(), group.getDescription(), group.getOfficeId(), group.getSharedLocAliasId(),
            group.getSharedRefLocationId());
        List<AssignedLocation> assignedLocations = group.getAssignedLocations();
        if(assignedLocations != null)
        {
            List<LOC_ALIAS_TYPE3> collect = assignedLocations.stream()
                .map(LocationGroupDao::convertToLocAliasType)
                .collect(toList());
            LOC_ALIAS_ARRAY3 assignedLocs = new LOC_ALIAS_ARRAY3(collect);
            CWMS_LOC_PACKAGE.call_ASSIGN_LOC_GROUPS3(configuration, categoryId, group.getId(),
                assignedLocs, group.getOfficeId());
        }
    }

    @NotNull
    private static LOC_ALIAS_TYPE3 convertToLocAliasType(AssignedLocation a) {
        return new LOC_ALIAS_TYPE3(a.getLocationId(),
            Optional.ofNullable(a.getAttribute()).map(n -> BigDecimal.valueOf(n.doubleValue())).orElse(null), a.getAliasId(), a.getRefLocationId());
    }
}
