package cwms.cda.data.dao;

import static usace.cwms.db.jooq.codegen.tables.AV_TS_EXTENTS_UTC.AV_TS_EXTENTS_UTC;

import org.jooq.Table;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;

public class CatalogRequestParameters {
    private final String office;
    private final String idLike;
    private final String unitSystem;
    private final String locCatLike;
    private final String locGroupLike;
    private final String tsCatLike;
    private final String tsGroupLike;
    private final String boundingOfficeLike;
    private final boolean includeExtents;
    private final boolean excludeEmpty;

    private CatalogRequestParameters(Builder builder) {
        this.office = builder.office;
        this.idLike = builder.idLike;
        this.unitSystem = builder.unitSystem;
        this.locCatLike = builder.locCatLike;
        this.locGroupLike = builder.locGroupLike;
        this.tsCatLike = builder.tsCatLike;
        this.tsGroupLike = builder.tsGroupLike;
        this.boundingOfficeLike = builder.boundingOfficeLike;
        this.includeExtents = builder.includeExtents;
        this.excludeEmpty = builder.excludeEmpty;
    }

    public String getBoundingOfficeLike() {
        return boundingOfficeLike;
    }

    public String getIdLike() {
        return idLike;
    }

    public boolean isIncludeExtents() {
        return includeExtents;
    }

    public String getLocCatLike() {
        return locCatLike;
    }

    public String getLocGroupLike() {
        return locGroupLike;
    }

    public String getOffice() {
        return office;
    }

    public String getTsCatLike() {
        return tsCatLike;
    }

    public String getTsGroupLike() {
        return tsGroupLike;
    }

    public String getUnitSystem() {
        return unitSystem;
    }

    public boolean isExcludeEmpty() {
        return excludeEmpty;
    }

    public static class Builder {
        String office;
        String idLike;
        String unitSystem;
        String locCatLike;
        String locGroupLike;
        String tsCatLike;
        String tsGroupLike;
        String boundingOfficeLike;
        boolean includeExtents = false;
        private boolean excludeEmpty = true;

        public Builder() {

        }

        public Builder withOffice(String office) {
            this.office = office;
            return this;
        }

        public Builder withIdLike(String idLike) {
            this.idLike = idLike;
            return this;
        }

        public Builder withUnitSystem(String unitSystem) {
            this.unitSystem = unitSystem;
            return this;
        }

        public Builder withLocCatLike(String locCatLike) {
            this.locCatLike = locCatLike;
            return this;
        }

        public Builder withLocGroupLike(String locGroupLike) {
            this.locGroupLike = locGroupLike;
            return this;
        }

        public Builder withTsCatLike(String tsCatLike) {
            this.tsCatLike = tsCatLike;
            return this;
        }

        public Builder withTsGroupLike(String tsGroupLike) {
            this.tsGroupLike = tsGroupLike;
            return this;
        }

        public Builder withBoundingOfficeLike(String boundingOfficeLike) {
            this.boundingOfficeLike = boundingOfficeLike;
            return this;
        }

        public Builder withIncludeExtents(boolean includeExtents) {
            this.includeExtents = includeExtents;
            return this;
        }

        public Builder withExcludeEmpty(boolean excludeExtents) {
            this.excludeEmpty = excludeExtents;
            return this;
        }

        public static Builder from(CatalogRequestParameters params) {
            // This NEEDS to include every field in the CatalogRequestParameters
            return new Builder()
                    .withOffice(params.office)
                    .withIdLike(params.idLike)
                    .withUnitSystem(params.unitSystem)
                    .withLocCatLike(params.locCatLike)
                    .withLocGroupLike(params.locGroupLike)
                    .withTsCatLike(params.tsCatLike)
                    .withTsGroupLike(params.tsGroupLike)
                    .withBoundingOfficeLike(params.boundingOfficeLike)
                    .withIncludeExtents(params.includeExtents)
                    .withExcludeEmpty(params.excludeEmpty)
                    ;
        }


        public CatalogRequestParameters build() {
            return new CatalogRequestParameters(this);
        }

    }

    // This is supposed to answer whether the current set of request parameters
    // needs the specified table to be joined into the query.
    public boolean needs(Table table) {
        if (table == AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN) {
            return locCatLike != null || locGroupLike != null;
        }

        if (table == AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN) {
            return tsCatLike != null || tsGroupLike != null;
        }

        if (table == AV_LOC.AV_LOC) {
            return boundingOfficeLike != null;
        }

        if (table == AV_TS_EXTENTS_UTC) {
            return includeExtents || excludeEmpty;
        }

        return false;
    }

}
