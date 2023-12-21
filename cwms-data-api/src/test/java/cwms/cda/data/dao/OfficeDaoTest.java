package cwms.cda.data.dao;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.data.dto.Office;
import java.sql.SQLException;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class OfficeDaoTest {

    @Test
    void getOfficeById() throws SQLException {
        DSLContext lrl = getDslContext(getConnection(), "LRL");

        OfficeDao dao = new OfficeDao(lrl);
        Office office = dao.getOfficeById("LRL").get();
        assertNotNull(office);

    }

}
