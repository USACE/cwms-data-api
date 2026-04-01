package cwms.cda.api.rating;

import cwms.cda.data.dao.RatingsVerticalDatumExtractor;
import cwms.cda.data.dao.VerticalDatum;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RatingsVerticalDatumExtractorTest {
    @Test
    void testGetVerticalDatum() throws Exception {
        String xml = RatingsControllerTestVerticalDatumIT.readVerticalDatumRatingXml(RatingsControllerTestVerticalDatumIT.LOC_WITH_NGVD29);
        Optional<VerticalDatum> datum = RatingsVerticalDatumExtractor.getVerticalDatum(xml);
        assertTrue(datum.isPresent());
        datum.ifPresent(vd -> assertSame(VerticalDatum.NGVD29, vd));
    }
}
