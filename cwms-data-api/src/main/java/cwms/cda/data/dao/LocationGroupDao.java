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

import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.noCondition;

import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.LocationGroup;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record10;
import org.jooq.RecordMapper;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSeekStep1;
import org.jooq.SelectSeekStep2;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.udt.records.LOC_ALIAS_ARRAY3;
import usace.cwms.db.jooq.codegen.udt.records.LOC_ALIAS_TYPE3;


public final class LocationGroupDao extends JooqDao<LocationGroup> {

    public static final String CWMS = "CWMS";
    private static final AV_LOC_GRP_ASSGN groupAssignView = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
    private static final AV_LOC_CAT_GRP catGroupView = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

    private final RecordMapper<Record10<String, String, String, String, String, String, BigDecimal,
        String, String, List<AssignedLocation>>, LocationGroup> mapToLocationGroup = queryRecord -> {
            LocationGroup group = buildLocationGroup(queryRecord);
            List<AssignedLocation> assignedLocations = (List<AssignedLocation>) queryRecord.get("multiset");
            return new LocationGroup(group, assignedLocations);
        };

    public LocationGroupDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Get a location group by office, category, and group id.
     * @param officeId The office id to use for the query.
     * @param categoryId The category id to use for the query.
     * @param groupId The group id to use for the query.
     * @return An optional location group.
     */
    public Optional<LocationGroup> getLocationGroup(@NotNull String officeId, @NotNull String categoryId,
                                                    @NotNull String groupId) {

        Condition joinCondition;
        if (CWMS.equalsIgnoreCase(officeId)) {
            joinCondition = DSL.noCondition();
        } else {
            joinCondition = groupAssignView.DB_OFFICE_ID.isNull().or(groupAssignView.DB_OFFICE_ID.eq(officeId));
        }

        Condition whereCondition = catGroupView.LOC_CATEGORY_ID.eq(categoryId)
            .and(catGroupView.LOC_GROUP_ID.eq(groupId))
            .and(catGroupView.GRP_DB_OFFICE_ID.in(CWMS, officeId))
            .and(catGroupView.CAT_DB_OFFICE_ID.in(CWMS, officeId));

        LocationGroup locGroup = buildQuery(whereCondition, joinCondition)
                .fetchSize(DEFAULT_FETCH_SIZE)
                .fetchOne(mapToLocationGroup);

        return Optional.ofNullable(locGroup);
    }

    private AssignedLocation buildAssignedLocation(Record resultRecord) {
        String locationId = resultRecord.get(groupAssignView.LOCATION_ID);
        String officeId = resultRecord.get(groupAssignView.DB_OFFICE_ID);

        String aliasId = resultRecord.get(groupAssignView.ALIAS_ID);
        Number attribute = resultRecord.get(groupAssignView.ATTRIBUTE);

        String refLocationId = resultRecord.get(groupAssignView.REF_LOCATION_ID);

        if (locationId == null) {
            return null;
        }
        return new AssignedLocation(locationId, officeId, aliasId, attribute, refLocationId);
    }

    private LocationGroup buildLocationGroup(Record resultRecord) {
        LocationCategory locationCategory = buildLocationCategory(resultRecord);

        String groupId = resultRecord.get(catGroupView.LOC_GROUP_ID);
        String sharedAliasId = resultRecord.get(catGroupView.SHARED_LOC_ALIAS_ID);
        String sharedRefLocationId = resultRecord.get(catGroupView.SHARED_REF_LOCATION_ID);

        String grpOfficeId = resultRecord.get(catGroupView.GRP_DB_OFFICE_ID);
        String grpDesc = resultRecord.get(catGroupView.LOC_GROUP_DESC);
        Number grpAttribute = resultRecord.get(catGroupView.LOC_GROUP_ATTRIBUTE);

        return new LocationGroup(locationCategory, grpOfficeId, groupId, grpDesc, sharedAliasId,
            sharedRefLocationId, grpAttribute);
    }

    private LocationCategory buildLocationCategory(Record resultRecord) {
        String catDbOfficeId = resultRecord.get(catGroupView.CAT_DB_OFFICE_ID);
        String categoryId = resultRecord.get(catGroupView.LOC_CATEGORY_ID);
        String catDesc = resultRecord.get(catGroupView.LOC_CATEGORY_DESC);
        return new LocationCategory(catDbOfficeId, categoryId, catDesc);
    }

    private SelectSeekStep2<Record10<String, String, String, String, String, String, BigDecimal,
        String, String, List<AssignedLocation>>, String, String> buildQuery(Condition whereCondition,
        Condition joinCondition) {
        return buildQuery(whereCondition, joinCondition, dsl);
    }

    private SelectSeekStep2<Record10<String, String, String, String, String, String, BigDecimal,
        String, String, List<AssignedLocation>>, String, String> buildQuery(Condition whereCondition,
            Condition joinCondition, DSLContext localDslContext) {

        // default join condition
        joinCondition = joinCondition.and(catGroupView.LOC_CATEGORY_ID.eq(groupAssignView.CATEGORY_ID)
            .and(catGroupView.LOC_GROUP_ID.eq(groupAssignView.GROUP_ID))
            .and(catGroupView.CAT_DB_OFFICE_ID.eq(groupAssignView.CATEGORY_OFFICE_ID))
            .and(catGroupView.GRP_DB_OFFICE_ID.eq(groupAssignView.GROUP_OFFICE_ID)));

        return localDslContext
            .select(
                catGroupView.CAT_DB_OFFICE_ID,
                catGroupView.LOC_CATEGORY_ID,
                catGroupView.LOC_CATEGORY_DESC,
                catGroupView.GRP_DB_OFFICE_ID,
                catGroupView.LOC_GROUP_ID,
                catGroupView.LOC_GROUP_DESC,
                catGroupView.LOC_GROUP_ATTRIBUTE,
                catGroupView.SHARED_LOC_ALIAS_ID,
                catGroupView.SHARED_REF_LOCATION_ID,
                DSL.multiset(
                    localDslContext.select(
                            groupAssignView.DB_OFFICE_ID,
                            groupAssignView.LOCATION_ID,
                            groupAssignView.ALIAS_ID,
                            groupAssignView.ATTRIBUTE,
                            groupAssignView.REF_LOCATION_ID
                        )
                        .from(groupAssignView)
                        .where(joinCondition)
                        .orderBy(groupAssignView.ATTRIBUTE)
                ).convertFrom(rs -> rs.map(this::buildAssignedLocation))
            )
            .from(catGroupView)
            .where(whereCondition)
            .orderBy(catGroupView.LOC_CATEGORY_ID,
                catGroupView.LOC_GROUP_ID);
    }

    /**
     * Get all location groups.
     * @return A list of all location groups.
     */
    public List<LocationGroup> getLocationGroups() {
        return getLocationGroups(null, null, null, null);
    }

    /**
     * Get all location groups for a given office.
     * @param officeId The office id to use for the query.
     * @return A list of all location groups for the given office.
     */
    public List<LocationGroup> getLocationGroups(String officeId) {
        return getLocationGroups(officeId, null, null, null);
    }

    public List<LocationGroup> getLocationGroups(String officeId, String categoryOfficeId, String locCategoryLike) {
        return getLocationGroups(officeId, null, categoryOfficeId, locCategoryLike);
    }

    /**
     * Get all location groups for a given office and category.
     * @param config The DSL configuration to use when querying the DB.
     * @param locationOfficeId The location office id to use for the query.
     * @param groupOfficeId The group office id to use for the query.
     * @param categoryOfficeId The category office id to use for the query.
     * @param locCategoryLike A regex to use to filter the location categories.  May be null.
     * @param sharedRefLocLike A regex to use to filter the shared_ref_location_id.  May be null.
     * @return A list of all location groups for the given office and category.
     */
    public List<LocationGroup> getLocationGroups(Configuration config, String locationOfficeId, String groupOfficeId,
            String categoryOfficeId, String locCategoryLike, String sharedRefLocLike) {

        Condition whereCondition = noCondition();

        if (locCategoryLike != null && !locCategoryLike.isEmpty()) {
            whereCondition = caseInsensitiveLikeRegex(catGroupView.LOC_CATEGORY_ID, locCategoryLike);
        }

        if (categoryOfficeId != null) {
            whereCondition = whereCondition.and(catGroupView.CAT_DB_OFFICE_ID.eq(categoryOfficeId.toUpperCase()));
        }

        if (sharedRefLocLike != null && !sharedRefLocLike.isEmpty()) {
            whereCondition = whereCondition
                .and(caseInsensitiveLikeRegex(catGroupView.SHARED_REF_LOCATION_ID, sharedRefLocLike));
        }

        whereCondition = whereCondition.and(catGroupView.LOC_GROUP_ID.isNotNull());

        Condition joinCondition = noCondition();

        if (groupOfficeId != null) {
            whereCondition = whereCondition
                .and(DSL.upper(catGroupView.GRP_DB_OFFICE_ID).eq(groupOfficeId.toUpperCase()));
        } else if (locationOfficeId != null) {
            joinCondition = joinCondition
                .and(DSL.upper(groupAssignView.DB_OFFICE_ID).eq(locationOfficeId.toUpperCase()));
        }

        DSLContext localDsl = DSL.using(config);

        return buildQuery(whereCondition, joinCondition, localDsl)
            .fetch(mapToLocationGroup);
    }

    /**
     * Get all location groups for a given office and category.
     * @param officeId The office id to use for the query.
     * @param includeAssigned Whether to include assigned locations in the results.
     * @param locCategoryLike A regex to use to filter the location categories.  May be null.
     * @return A list of all location groups for the given office and category.
     */
    public List<LocationGroup> getLocationGroups(@Nullable String officeId, String groupOfficeId,
                                                 String categoryOfficeId, boolean includeAssigned,
                                                 @Nullable String locCategoryLike) {
        if (includeAssigned) {
            return getLocationGroups(officeId, groupOfficeId, categoryOfficeId, locCategoryLike);
        } else {
            return getGroupsWithoutAssignedLocations(officeId, categoryOfficeId, locCategoryLike);
        }
    }

    /**
     * Get all location groups for a given office and category,
     * as well as a where clause to filter the shared_ref_location_id.
     * @param locationOfficeId The office id to use for the query.
     * @param groupOfficeId The office id to use for the query.
     * @param locCategoryLike A regex to use to filter the location categories.  May be null.
     * @param sharedRefLocLike A where clause to filter the shared_loc_alias_id.  May be null.
     * @return A list of all location groups for the given parameters.
     */

    public List<LocationGroup> getLocationGroups(String locationOfficeId, String groupOfficeId, String categoryOfficeId,
            String locCategoryLike, String sharedRefLocLike) {

        Condition whereCondition = noCondition();

        if (locCategoryLike != null && !locCategoryLike.isEmpty()) {
            whereCondition = caseInsensitiveLikeRegex(catGroupView.LOC_CATEGORY_ID, locCategoryLike);
        }

        if (categoryOfficeId != null) {
            whereCondition = whereCondition.and(catGroupView.CAT_DB_OFFICE_ID.eq(categoryOfficeId.toUpperCase()));
        }

        if (sharedRefLocLike != null && !sharedRefLocLike.isEmpty()) {
            whereCondition = whereCondition
                .and(caseInsensitiveLikeRegex(catGroupView.SHARED_REF_LOCATION_ID, sharedRefLocLike));
        }

        whereCondition = whereCondition.and(catGroupView.LOC_GROUP_ID.isNotNull());

        Condition joinCondition = noCondition();

        if (groupOfficeId != null) {
            whereCondition = whereCondition
                .and(DSL.upper(catGroupView.GRP_DB_OFFICE_ID).eq(groupOfficeId.toUpperCase()));
        } else if (locationOfficeId != null) {
            joinCondition = joinCondition
                .and(DSL.upper(groupAssignView.DB_OFFICE_ID).eq(locationOfficeId.toUpperCase()));
        }

        return buildQuery(whereCondition, joinCondition)
            .fetch(mapToLocationGroup);
    }

    /**
     * Get all location groups for a given office and category.
     * @param locationOfficeId The group office id to use for the query.
     * @param groupOfficeId The group office id to use for the query.
     * @param categoryOfficeId The category office id to use for the query.
     * @param locCategoryLike A regex to use to filter the location categories.  May be null.
     * @return A list of all location groups for the given office and category.
     */
    public List<LocationGroup> getLocationGroups(String locationOfficeId, String groupOfficeId,
            String categoryOfficeId, String locCategoryLike) {

        Condition whereCondition = noCondition();
        if (locCategoryLike != null && !locCategoryLike.isEmpty()) {
            whereCondition = caseInsensitiveLikeRegex(catGroupView.LOC_CATEGORY_ID, locCategoryLike);
        }

        if (categoryOfficeId != null) {
            whereCondition = whereCondition.and(catGroupView.CAT_DB_OFFICE_ID.eq(categoryOfficeId.toUpperCase()));
        }

        Condition joinCondition = noCondition();

        if (locationOfficeId != null) {
            if (CWMS.equalsIgnoreCase(locationOfficeId)) {
                whereCondition = whereCondition.and(catGroupView.CAT_DB_OFFICE_ID.eq(CWMS)
                        .and(catGroupView.GRP_DB_OFFICE_ID.eq(CWMS))
                );
            } else {
                if (groupOfficeId != null) {
                    whereCondition = whereCondition.and(catGroupView.CAT_DB_OFFICE_ID.in(CWMS, locationOfficeId)
                            .and(catGroupView.GRP_DB_OFFICE_ID.in(CWMS, groupOfficeId))

                    );
                    joinCondition = joinCondition.and(groupAssignView.DB_OFFICE_ID.isNull()
                            .or(groupAssignView.DB_OFFICE_ID.eq(locationOfficeId))
                    );
                } else {
                    whereCondition = whereCondition.and(catGroupView.CAT_DB_OFFICE_ID.in(CWMS, locationOfficeId));
                    joinCondition = joinCondition.and(groupAssignView.DB_OFFICE_ID.isNull()
                        .or(groupAssignView.DB_OFFICE_ID.eq(locationOfficeId))
                    );
                }
            }
        } else {
            whereCondition = whereCondition.and(catGroupView.LOC_GROUP_ID.isNotNull());
        }

        return buildQuery(whereCondition, joinCondition)
            .fetch(mapToLocationGroup);
    }

    private List<LocationGroup> getGroupsWithoutAssignedLocations(
            @Nullable String groupOfficeId, @Nullable String categoryOfficeId,
            @Nullable String locCategoryLike) {
        TableField[] columns = new TableField[]{catGroupView.CAT_DB_OFFICE_ID, catGroupView.LOC_CATEGORY_ID,
            catGroupView.LOC_CATEGORY_DESC, catGroupView.GRP_DB_OFFICE_ID, catGroupView.LOC_GROUP_ID,
            catGroupView.LOC_GROUP_DESC, catGroupView.SHARED_LOC_ALIAS_ID, catGroupView.SHARED_REF_LOCATION_ID,
            catGroupView.LOC_GROUP_ATTRIBUTE};

        Condition condition = catGroupView.LOC_GROUP_ID.isNotNull();
        if (groupOfficeId != null && !groupOfficeId.isEmpty()) {
            condition = condition.and(catGroupView.GRP_DB_OFFICE_ID.eq(groupOfficeId));
        }

        if (categoryOfficeId != null && !categoryOfficeId.isEmpty()) {
            condition = condition.and(catGroupView.CAT_DB_OFFICE_ID.eq(categoryOfficeId));
        }

        if (locCategoryLike != null && !locCategoryLike.isEmpty()) {
            condition = condition.and(caseInsensitiveLikeRegex(catGroupView.LOC_CATEGORY_ID, locCategoryLike));
        }

        SelectJoinStep<Record> step = dsl.selectDistinct(columns).from(catGroupView);

        return step.where(condition)
                .orderBy(catGroupView.LOC_CATEGORY_ID, catGroupView.LOC_GROUP_ATTRIBUTE, catGroupView.LOC_GROUP_ID)
                .fetchSize(1000)
                .fetch()
                .map(this::buildLocationGroup);
    }

    public Feature buildFeatureFromAvLocRecordWithLocGroup(Record avLocRecord) {

        List<Field<?>> fieldsInRecord = Arrays.asList(avLocRecord.fields());

        Set<TableField<?, ?>> grpAssgnFields = new LinkedHashSet<>();
        grpAssgnFields.add(groupAssignView.CATEGORY_ID);
        grpAssgnFields.add(groupAssignView.GROUP_ID);
        grpAssgnFields.add(groupAssignView.ATTRIBUTE);
        grpAssgnFields.add(groupAssignView.ALIAS_ID);
        grpAssgnFields.add(groupAssignView.SHARED_ALIAS_ID);
        grpAssgnFields.add(groupAssignView.SHARED_REF_LOCATION_ID);

        grpAssgnFields.retainAll(fieldsInRecord);

        Map<String, Object> grpProps = new LinkedHashMap<>();
        grpAssgnFields.forEach(f -> grpProps.put(f.getName(), avLocRecord.getValue(f)));

        Feature feature = LocationsDaoImpl.buildFeatureFromAvLocRecord(avLocRecord);
        Map<String, Object> props = feature.getProperties();
        props.put("avLocGrpAssgn", grpProps);
        feature.setProperties(props);
        return feature;
    }

    public FeatureCollection buildFeatureCollectionForLocationGroup(String locationOfficeId, String groupOfficeId,
            String categoryOfficeId,
            String categoryId,
            String groupId, String units) {
        AV_LOC al = AV_LOC.AV_LOC;

        SelectSeekStep1<Record, BigDecimal> select = dsl.select(al.asterisk(), groupAssignView.CATEGORY_ID,
                        groupAssignView.GROUP_ID, groupAssignView.ATTRIBUTE, groupAssignView.ALIAS_ID,
                        groupAssignView.SHARED_REF_LOCATION_ID, groupAssignView.SHARED_ALIAS_ID)
                .from(al).join(groupAssignView).on(al.LOCATION_ID.eq(groupAssignView.LOCATION_ID))
                .where(groupAssignView.DB_OFFICE_ID.eq(locationOfficeId)
                        .and(groupAssignView.CATEGORY_OFFICE_ID.eq(categoryOfficeId))
                        .and(groupAssignView.GROUP_OFFICE_ID.eq(groupOfficeId))
                        .and(groupAssignView.CATEGORY_ID.eq(categoryId)
                                .and(groupAssignView.GROUP_ID.eq(groupId))
                                .and(al.UNIT_SYSTEM.eq(units))))
                .orderBy(groupAssignView.ATTRIBUTE);

        List<Feature> features =
                select.stream()
                        .map(this::buildFeatureFromAvLocRecordWithLocGroup)
                        .collect(toList());
        FeatureCollection collection = new FeatureCollection();
        collection.setFeatures(features);

        return collection;
    }

    /**
     * Delete a location group.
     * @param categoryId The category id to use for the query.
     * @param groupId The group id to use for the query.
     * @param cascadeDelete Whether to cascade the delete.
     * @param office The office id to use for the query.
     */
    public void delete(String categoryId, String groupId, boolean cascadeDelete, String office) {
        connection(dsl, conn -> {
            DSLContext dslContext = getDslContext(conn, office);
            CWMS_LOC_PACKAGE.call_DELETE_LOC_GROUP__2(dslContext.configuration(), categoryId,
                    groupId, formatBool(cascadeDelete), office);
        });
    }

    /**
     * Create a location group.
     * @param group The location group to create.
     */
    public void create(LocationGroup group) {
        String office =  group.getOfficeId();
        String categoryId = group.getLocationCategory().getId();

        connection(dsl, conn -> {
            DSLContext dslContext = getDslContext(conn, office);
            dslContext.transaction((Configuration trx) -> {
                Configuration config = trx.dsl().configuration();
                CWMS_LOC_PACKAGE.call_CREATE_LOC_GROUP2(config, categoryId,
                    group.getId(), group.getDescription(), group.getOfficeId(), group.getSharedLocAliasId(),
                    group.getSharedRefLocationId());
                assignLocs(config, group, office);
            });
        });
    }

    @NotNull
    private static LOC_ALIAS_TYPE3 convertToLocAliasType(AssignedLocation a) {
        BigDecimal attribute = toBigDecimal(a.getAttribute());
        return new LOC_ALIAS_TYPE3(a.getLocationId(),
                attribute, a.getAliasId(), a.getRefLocationId());
    }

    /**
     * Update a location group.
     * @param oldGroupId The old group id.
     * @param newGroup The new location group id.
     */
    public void renameLocationGroup(String oldGroupId, LocationGroup newGroup) {
        String office = newGroup.getOfficeId();

        connection(dsl, conn -> {
            DSLContext dslContext = getDslContext(conn, office);
            CWMS_LOC_PACKAGE.call_RENAME_LOC_GROUP(dslContext.configuration(), newGroup.getLocationCategory().getId(),
                    oldGroupId, newGroup.getId(), newGroup.getDescription(), "T", office);
        });
    }

    public void unassignAllLocs(LocationGroup group, String office) {
        LocationCategory cat = group.getLocationCategory();
        connection(dsl, conn -> {
            DSLContext dslContext = getDslContext(conn, office);
            CWMS_LOC_PACKAGE.call_UNASSIGN_LOC_GROUP(dslContext.configuration(),
                    cat.getId(), group.getId(), null, "T", office);
        });
    }

    public void assignLocs(LocationGroup group, String office) {
        connection(dsl, conn -> {
            DSLContext dslContext = getDslContext(conn, office);
            assignLocs(dslContext.configuration(), group, office);
        });
    }

    /**
     * Used when an appropriate context already exists to avoid opening a second connection.
     * @param config a DSL configuration to use for the operation
     * @param group the location group to assign locations to
     * @param office the office to use for the operation
     */
    public void assignLocs(Configuration config, LocationGroup group, String office) {
        List<AssignedLocation> assignedLocations = group.getAssignedLocations();
        if (assignedLocations != null) {
            List<LOC_ALIAS_TYPE3> collect = assignedLocations.stream()
                .map(
                    item -> {
                        if (item.getLocationId() == null || item.getOfficeId() == null) {
                            throw new IllegalArgumentException("Invalid assigned location. Required fields are null.");
                        } else {
                            return item;
                        }
                    })
                    .map(LocationGroupDao::convertToLocAliasType)
                    .collect(toList());
            LOC_ALIAS_ARRAY3 assignedLocs = new LOC_ALIAS_ARRAY3(collect);
            LocationCategory cat = group.getLocationCategory();
            CWMS_LOC_PACKAGE.call_ASSIGN_LOC_GROUPS3(config, cat.getId(), group.getId(), assignedLocs, office);
        }
    }
}
