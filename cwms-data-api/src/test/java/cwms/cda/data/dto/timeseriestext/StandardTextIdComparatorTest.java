package cwms.cda.data.dto.timeseriestext;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StandardTextIdComparatorTest {

    @Test
    void testCompareSelf(){
        StandardTextId id = new StandardTextId.Builder()
                .withId("theId")
                .withOfficeId("theOffice").build();

        StandardTextIdComparator comparator = new StandardTextIdComparator();
        assertEquals(0, comparator.compare(id, id));
    }

    @Test
    void testCompareDiffOffice(){
        StandardTextId.Builder builder = new StandardTextId.Builder()
                .withId("theId");

        StandardTextId idA = builder.withOfficeId("OfficeA").build();
        StandardTextId idB = builder.withOfficeId("OfficeB").build();

        StandardTextIdComparator comparator = new StandardTextIdComparator();
        assertEquals(-1, comparator.compare(idA, idB));
        assertEquals(1, comparator.compare(idB, idA));
    }

    @Test
    void testCompareNullOffices(){
        StandardTextId.Builder builder = new StandardTextId.Builder()
                .withId("theId");

        StandardTextId idA = builder.withOfficeId(null).build();
        StandardTextId idB = builder.withOfficeId(null).build();
        StandardTextId idC = builder.withOfficeId("OfficeA").build();

        StandardTextIdComparator comparator = new StandardTextIdComparator();
        assertEquals(0, comparator.compare(idA, idB));
        assertEquals(0, comparator.compare(idB, idA));
        assertEquals(-1, comparator.compare(idA, idC));  // nulls come first
        assertEquals(1, comparator.compare(idC, idA));
    }

    @Test
    void testCompareEmptyOffices(){
        StandardTextId.Builder builder = new StandardTextId.Builder()
                .withId("theId");

        StandardTextId idA = builder.withOfficeId("").build();
        StandardTextId idB = builder.withOfficeId("").build();
        StandardTextId idC = builder.withOfficeId("OfficeA").build();

        StandardTextIdComparator comparator = new StandardTextIdComparator();
        assertEquals(0, comparator.compare(idA, idB));
        assertEquals(0, comparator.compare(idB, idA));
        assertTrue(0> comparator.compare(idA, idC));
        assertTrue(0< comparator.compare(idC, idA));
    }

    @Test
    void testCompareDiffID(){
        StandardTextId.Builder builder = new StandardTextId.Builder()
                .withOfficeId("SWT");

        StandardTextId idA = builder.withId("A").build();
        StandardTextId idB = builder.withId("B").build();

        StandardTextIdComparator comparator = new StandardTextIdComparator();
        assertEquals(-1, comparator.compare(idA, idB));
        assertEquals(1, comparator.compare(idB, idA));
    }

}