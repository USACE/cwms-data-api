//package cwms.cda.api.rating;
//
//import cwms.cda.data.dao.VerticalDatum;
//import hec.data.cwmsRating.RatingSet;
//import hec.data.cwmsRating.io.RatingSetContainer;
//import java.io.File;
//import java.io.IOException;
//import java.net.URL;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import mil.army.usace.hec.cwms.rating.io.xml.RatingSetContainerXmlFactory;
//import org.jetbrains.annotations.NotNull;
//import org.junit.jupiter.api.Test;
//import static org.junit.jupiter.api.Assertions.*;
//
//public class DeleteMeTest {
//    static final String BASE_LOCATION = "RatingDatumTest";
//    static final String LOC_WITH_NAVD88 = BASE_LOCATION + "-NAVD88";
//    static final String LOC_WITH_NGVD29 = BASE_LOCATION + "-NGVD29";
//
//    protected static String readResourceFile(String resourcePath) throws IOException {
//        URL resource = DeleteMeTest.class.getClassLoader().getResource(resourcePath);
//        if (resource == null) {
//            throw new IOException("Resource not found: " + resourcePath);
//        }
//        Path path = new File(resource.getFile()).toPath();
//        return String.join("\n", Files.readAllLines(path));
//    }
//
//    static @NotNull String readVerticalDatumRatingXml(String location) throws IOException {
//        return readResourceFile("cwms/cda/api/vertical_datum_example_rating.xml").replace("{office-id}", "SPK")
//                                                                                 .replace("{location}", location);
//    }
//
//    @Test
//    void test() throws Exception {
//        String xml = readVerticalDatumRatingXml(LOC_WITH_NGVD29);
//        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(xml);
//        RatingSet rs = new RatingSet(container);
//        RatingSet rsNew = new RatingSet(container);
//
//        assertTrue(rs.toNAVD88());
//        assertNotEquals(rs.getRatings()[0].getRatingExtents()[0][0], rsNew.getRatings()[0].getRatingExtents()[0][0], 0.01);
//
//        VerticalDatum vd = VerticalDatum.getVerticalDatum(rs.getNativeVerticalDatum());
//        assertEquals(VerticalDatum.NGVD29, vd);
//
//        vd = VerticalDatum.getVerticalDatum(rs.getCurrentVerticalDatum());
//        assertEquals(VerticalDatum.NAVD88, vd);
//    }
//}
