package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BlobDaoTest {

    @Test
    void testGetLength() {
        // Full range (end=null): offset=0, end=null, totalLength=10 -> length=10
        assertEquals(10L, BlobDao.getLength(0L, null, 10L));

        // suffix but we expect method to treat end as full end not the suffix version.
        assertEquals(10L, BlobDao.getLength(0L, -1L, 10L));

        assertEquals(1L, BlobDao.getLength(0L, 0L, 10L));
        assertEquals(2L, BlobDao.getLength(0L, 1L, 10L));

        // Normal range: offset=0, end=9, totalLength=10 -> length=10
        assertEquals(10L, BlobDao.getLength(0L, 9L, 10L));

        // Sub range from middle: offset=5, end=9, totalLength=10 -> length=5
        assertEquals(5L, BlobDao.getLength(5L, 9L, 10L));

        // Offset from middle, end is null: offset=50, end=null, totalLength=10 -> length=50
        assertEquals(6L, BlobDao.getLength(4L, null, 10L));
        assertEquals(6L, BlobDao.getLength(4L, 9L, 10L));

        assertEquals(1L, BlobDao.getLength(-1L, 1L, 10L));

    }
}
