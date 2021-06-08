package cwms.radar.data.dto.catalog;

import java.util.List;

import usace.cwms.db.jooq.codegen.tables.records.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.records.AV_LOC_ALIAS;

public class CatalogIntermediate {
    public AV_LOC location;
    public List<AV_LOC_ALIAS> aliases;
}
