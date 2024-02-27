package cwms.cda.data.dao.texttimeseries;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TimeSeriesTextModeTest {

    @Test
    public void testGetMode() {
        TimeSeriesTextMode mode = TimeSeriesTextMode.getMode("ALL");
        assertNotNull(mode);
        assertEquals(TimeSeriesTextMode.ALL, mode);

        mode = TimeSeriesTextMode.getMode("STANDARD");
        assertNotNull(mode);
        assertEquals(TimeSeriesTextMode.STANDARD, mode);

        mode = TimeSeriesTextMode.getMode("REGULAR");
        assertNotNull(mode);
        assertEquals(TimeSeriesTextMode.REGULAR, mode);

    }

}