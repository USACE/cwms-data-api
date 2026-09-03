package cwms.cda.data.dao;

import cwms.cda.data.dto.SpecifiedLevel;

import java.util.List;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;


public final class SpecifiedLevelDao extends JooqDao<SpecifiedLevel> {
    private static final Field<String> OFFICE_ID_F = field(name("OFFICE_ID"), String.class);
    private static final Field<String> SPECIFIED_LEVEL_ID_F = field(name("SPECIFIED_LEVEL_ID"), String.class);
    private static final Field<String> DESCRIPTION_F = field(name("DESCRIPTION"), String.class);


    public SpecifiedLevelDao(DSLContext dsl) {
        super(dsl);
    }


    public List<SpecifiedLevel> getSpecifiedLevels(String office, String templateIdMask) {
        return connectionResult(dsl, c -> {
            Configuration configuration = getDslContext(c, office).configuration();
            Result<Record> records = CWMS_LEVEL_PACKAGE.call_CAT_SPECIFIED_LEVELS(configuration, templateIdMask, office);
            return records.map(r -> new SpecifiedLevel(
                    r.get(SPECIFIED_LEVEL_ID_F),
                    r.get(OFFICE_ID_F),
                    r.get(DESCRIPTION_F)
            ));
        });
    }

    public void create(SpecifiedLevel specifiedLevel, boolean failIfExists) {
        connection(dsl, c ->
                CWMS_LEVEL_PACKAGE.call_STORE_SPECIFIED_LEVEL(
                        getDslContext(c, specifiedLevel.getOfficeId()).configuration(),
                        specifiedLevel.getId(), specifiedLevel.getDescription(), formatBool(failIfExists),
                        specifiedLevel.getOfficeId())
        );

    }

    public void update(String oldSpecifiedLevelId, String newSpecifiedLevelId, String officeId) {
        connection(dsl, c -> CWMS_LEVEL_PACKAGE.call_RENAME_SPECIFIED_LEVEL(
                getDslContext(c, officeId).configuration(),
                oldSpecifiedLevelId, newSpecifiedLevelId, officeId)
        );


    }

    public void delete(String specifiedLevelId, String office) {
        String failIfNotFound = formatBool(true);
        connection(dsl, c -> CWMS_LEVEL_PACKAGE.call_DELETE_SPECIFIED_LEVEL(
                getDslContext(c, office).configuration(),
                specifiedLevelId, failIfNotFound, office)
        );
    }
}
