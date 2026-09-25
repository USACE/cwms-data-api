package cwms.cda.data.dao.forecast;

import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsDTOBase;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_FCST_PACKAGE;

import java.util.List;

/**
 * Shared logic for the forecast spec DAOs.
 *
 * <p>{@link ForecastSpecDaoV1} backs the V1 API, where a forecast spec has a single
 * {@code location-id}. {@link ForecastSpecDaoV2} backs the V2 API, where a forecast
 * spec has a {@code List<ForecastLocation>} (each with its own sort order and a
 * primary-location flag). Everything below does not care which of those two shapes
 * {@code T} is -- deleting a spec is the same statement either way, and the
 * office/spec-id/designator/source-entity filtering used by both DAOs' "get" queries
 * is identical -- so it lives here once instead of being copy-pasted between the two
 * DAOs and risking drift.
 *
 * <p>What does NOT live here, because it genuinely differs between V1 and V2, is:
 * building/mapping the spec projection itself (V1 joins a single location column in;
 * V2 fetches locations separately and reassembles them, see {@link ForecastSpecDaoV2}),
 * and {@code create}/{@code update}, since storing a spec's locations is shaped
 * differently for a single id vs. a list.
 *
 * @param <T> the forecast spec DTO type this instance works with
 */
public abstract class ForecastSpecDao<T extends CwmsDTOBase> extends JooqDao<T> {

    protected ForecastSpecDao(DSLContext dsl) {
        super(dsl);
    }

    protected abstract ViewWrapper getViewWrapper();

    protected static class ViewWrapper {
        protected final TableField<?, String> OFFICE_ID;
        protected final TableField<?, String> FCST_SPEC_ID;
        protected final TableField<?, String> FCST_DESIGNATOR;
        protected final TableField<?, String> ENTITY_ID;
        protected final TableField<?, ?> FCST_SPEC_CODE;

        public ViewWrapper(TableField<?, String> officeId, TableField<?, String> specId,
                TableField<?, String> designator, TableField<?, String> entityId,
                TableField<?, ?> fcstSpecCode) {
            this.OFFICE_ID = officeId;
            this.FCST_SPEC_ID = specId;
            this.FCST_DESIGNATOR = designator;
            this.ENTITY_ID = entityId;
            this.FCST_SPEC_CODE = fcstSpecCode;
        }
    }

    /**
     * Deletes a forecast spec. What locations look like is irrelevant to delete,
     * so V1 and V2 share this method unchanged.
     */
    public void delete(String office, String specId, String designator, DeleteRule deleteRule) {
        connection(dsl, conn -> {
            setOffice(conn, office);
            CWMS_FCST_PACKAGE.call_DELETE_FCST_SPEC(DSL.using(conn).configuration(), specId, designator,
                    deleteRule.getRule(), office);
        });
    }

    /**
     * Source-entity filter shared by both DAOs' {@code getForecastSpecs} (plural, filtered
     * listing) queries: LIKE-style matching when {@code entityLike} is given, otherwise a
     * regex match against {@code sourceEntityRegex}.
     */
    protected static Condition buildEntityCondition(ViewWrapper spec, String sourceEntityRegex,
            String entityLike) {
        if (entityLike != null) {
            return spec.ENTITY_ID.likeIgnoreCase(entityLike);
        }
        return JooqDao.caseInsensitiveLikeRegex(spec.ENTITY_ID, sourceEntityRegex);
    }

    /**
     * Designator filter for {@code getForecastSpecs} (plural): designator is a nullable
     * column, and a null filter means "specs with no designator" rather than "any
     * designator." A non-null filter is treated as a mask/regex, matching the
     * {@code DESIGNATOR_MASK} query param both controllers pass through here.
     */
    protected static Condition buildDesignatorMaskCondition(ViewWrapper spec, String designatorMask) {
        if (designatorMask == null) {
            return spec.FCST_DESIGNATOR.isNull();
        }
        return JooqDao.caseInsensitiveLikeRegex(spec.FCST_DESIGNATOR, designatorMask);
    }

    /**
     * Designator filter for {@code getForecastSpec} (singular, fetch-by-key): unlike the
     * plural listing query, this is an exact match, matching the plain {@code DESIGNATOR}
     * query param both controllers pass through here.
     */
    protected static Condition buildExactDesignatorCondition(ViewWrapper spec, String designator) {
        if (designator == null) {
            return spec.FCST_DESIGNATOR.isNull();
        }
        return spec.FCST_DESIGNATOR.eq(designator);
    }

    /**
     * The full office/spec-id/designator/source-entity filter used by both DAOs'
     * {@code getForecastSpecs} (plural) queries. Only the projected columns and how
     * locations are joined/fetched differ between V1 and V2.
     */
    protected static Condition buildSpecListCondition(ViewWrapper spec, String office, String specIdRegex,
            String designatorMask, String sourceEntityRegex, String entityLike) {
        return JooqDao.caseInsensitiveLikeRegex(spec.OFFICE_ID, office)
                .and(JooqDao.caseInsensitiveLikeRegex(spec.FCST_SPEC_ID, specIdRegex))
                .and(buildEntityCondition(spec, sourceEntityRegex, entityLike))
                .and(buildDesignatorMaskCondition(spec, designatorMask));
    }

    /**
     * The office/spec-id/designator key filter used by both DAOs' {@code getForecastSpec}
     * (singular, fetch-by-key) queries. Office and spec id are exact matches (office
     * upper-cased, matching the existing V1 behavior); designator uses the exact-match
     * rule above rather than the mask used by the plural listing query.
     */
    protected static Condition buildSpecKeyCondition(ViewWrapper spec, String office, String specId,
            String designator) {
        return spec.OFFICE_ID.eq(office.toUpperCase())
                .and(spec.FCST_SPEC_ID.eq(specId))
                .and(buildExactDesignatorCondition(spec, designator));
    }


    public abstract List<T> getForecastSpecs(String office, String specIdRegex, String designator,
            String sourceEntityRegex, String entityLike);

    public abstract T getForecastSpec(String office, String specId, String designator);

    public abstract void create(T forecastSpec);

    public abstract void update(T forecastSpec);
}
