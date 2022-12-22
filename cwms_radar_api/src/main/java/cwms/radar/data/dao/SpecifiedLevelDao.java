package cwms.radar.data.dao;

import cwms.radar.data.dto.SpecifiedLevel;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.jooq.DSLContext;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.dao.CwmsDbLevelJooq;

public class SpecifiedLevelDao extends JooqDao<SpecifiedLevel> {
    private static final Logger logger = Logger.getLogger(SpecifiedLevelDao.class.getName());
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
}
