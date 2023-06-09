package cwms.cda.data.dao;

import cwms.cda.data.dto.SpecifiedLevel;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.jooq.DSLContext;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;
import usace.cwms.db.jooq.dao.CwmsDbLevelJooq;

public final class SpecifiedLevelDao extends JooqDao<SpecifiedLevel> {
    public static final String OFFICE_ID = "OFFICE_ID";
    public static final String SPECIFIED_LEVEL_ID = "SPECIFIED_LEVEL_ID";
    public static final String DESCRIPTION = "DESCRIPTION";

    public SpecifiedLevelDao(DSLContext dsl) {
        super(dsl);
    }


    public List<SpecifiedLevel> getSpecifiedLevels(String office, String templateIdMask) {
        List<SpecifiedLevel> retval = new ArrayList<>();

        CwmsDbLevelJooq jooq = new CwmsDbLevelJooq();
        connection(dsl, c -> {
            ResultSet rs = jooq.catSpecifiedLevels(c, templateIdMask, office);
            final List<String> specLevelColumnsList = Arrays.asList(OFFICE_ID, SPECIFIED_LEVEL_ID,
                    DESCRIPTION);
            OracleTypeMap.checkMetaData(rs.getMetaData(), specLevelColumnsList, "Specified Level");
            while (rs.next()) {
                String officeId = rs.getString(OFFICE_ID);
                String id = rs.getString(SPECIFIED_LEVEL_ID);
                String desc = rs.getString(DESCRIPTION);

                SpecifiedLevel specLevel = new SpecifiedLevel(id, officeId, desc);

                retval.add(specLevel);
            }
        });

        return retval;
    }

    public void create(SpecifiedLevel specifiedLevel, boolean failIfExists) {
        try {
        dsl.connection(c -> 
            CWMS_LEVEL_PACKAGE.call_STORE_SPECIFIED_LEVEL(
                getDslContext(c,specifiedLevel.getOfficeId()).configuration(),
                specifiedLevel.getId(), specifiedLevel.getDescription(), OracleTypeMap.formatBool(failIfExists),
                specifiedLevel.getOfficeId())
        );
        } catch(RuntimeException ex) {
            throw wrapException(ex);
        }
        
    }

    public void update(String oldSpecifiedLevelId, String newSpecifiedLevelId, String officeId) {
        try {
            dsl.connection(c->
            CWMS_LEVEL_PACKAGE.call_RENAME_SPECIFIED_LEVEL(
                getDslContext(c,officeId).configuration(),
                oldSpecifiedLevelId, newSpecifiedLevelId, officeId)
            );
        } catch(RuntimeException ex) {
            throw wrapException(ex);
        }
        
        
    }

    public void delete(String specifiedLevelId, String office) {
        String failIfNotFound = OracleTypeMap.formatBool(true);
        try {
            dsl.connection(c->
                CWMS_LEVEL_PACKAGE.call_DELETE_SPECIFIED_LEVEL(
                    getDslContext(c,office).configuration(), specifiedLevelId, failIfNotFound,
                    office)
            );
        } catch(RuntimeException ex) {
            throw wrapException(ex);
        }
        
    }
}
