package cwms.cda.data.dao;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.data.dto.Office;
import java.sql.SQLException;
import java.util.Optional;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class OfficeDaoTest {

    @Test
    void getOfficeById() throws SQLException {
        DSLContext lrl = getDslContext(getConnection(), "LRL");

        OfficeDao dao = new OfficeDao(lrl);
        Optional<Office> officeOpt = dao.getOfficeById("LRL");
        assertTrue(officeOpt.isPresent());
        Office office = officeOpt.get();
        assertNotNull(office);

    }

}
