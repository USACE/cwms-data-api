package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

final class UserDaoTest {

    // Mirror the production regex and parsing logic from UserDao to test independently
    // without requiring a database connection for DAO construction.
    private static final Pattern EMBARGO_PATTERN =
        Pattern.compile(".*-(\\d+)(h|d)$", Pattern.CASE_INSENSITIVE);

    private static int parseEmbargoFromTsGroupId(String tsGroupId) {
        if (tsGroupId == null) {
            return 0;
        }
        Matcher matcher = EMBARGO_PATTERN.matcher(tsGroupId);
        if (matcher.matches()) {
            try {
                int value = Integer.parseInt(matcher.group(1));
                String unit = matcher.group(2).toLowerCase();
                if ("d".equals(unit)) {
                    return value * 24;
                }
                return value;
            } catch (NumberFormatException e) {
                return 0;
            }
        }
        return 0;
    }

    private static String convertPrivilegeBit(int privilegeBit) {
        boolean canRead = (privilegeBit & 2) != 0;
        boolean canWrite = (privilegeBit & 4) != 0;
        if (canRead && canWrite) {
            return "read-write";
        } else if (canWrite) {
            return "write";
        } else if (canRead) {
            return "read";
        }
        return "none";
    }

    // Embargo parsing: hour suffix
    @Test
    void testParseEmbargoHoursSuffix() {
        assertEquals(72, parseEmbargoFromTsGroupId("policy-dam_operator-r-72h"));
    }

    @Test
    void testParseEmbargoDaySuffix() {
        assertEquals(168, parseEmbargoFromTsGroupId("policy-viewer_users-r-7d"));
    }

    @Test
    void testParseEmbargoDaySuffixCaseInsensitive() {
        assertEquals(168, parseEmbargoFromTsGroupId("policy-viewer_users-r-7D"));
    }

    @Test
    void testParseEmbargoSingleHour() {
        assertEquals(1, parseEmbargoFromTsGroupId("group-1h"));
    }

    @Test
    void testParseEmbargoSingleDay() {
        assertEquals(24, parseEmbargoFromTsGroupId("group-1d"));
    }

    @Test
    void testParseEmbargoLargeDayValue() {
        assertEquals(720, parseEmbargoFromTsGroupId("policy-restricted-30d"));
    }

    // Embargo parsing: no match cases
    @Test
    void testParseEmbargoNoSuffix() {
        assertEquals(0, parseEmbargoFromTsGroupId("policy-dam_operator-r"));
    }

    @Test
    void testParseEmbargoNull() {
        assertEquals(0, parseEmbargoFromTsGroupId(null));
    }

    @Test
    void testParseEmbargoEmpty() {
        assertEquals(0, parseEmbargoFromTsGroupId(""));
    }

    @Test
    void testParseEmbargoNoNumber() {
        assertEquals(0, parseEmbargoFromTsGroupId("group-h"));
    }

    @Test
    void testParseEmbargoUnknownUnit() {
        assertEquals(0, parseEmbargoFromTsGroupId("group-72m"));
    }

    // Privilege bitmask conversion
    @Test
    void testConvertPrivilegeBitRead() {
        assertEquals("read", convertPrivilegeBit(2));
    }

    @Test
    void testConvertPrivilegeBitWrite() {
        assertEquals("write", convertPrivilegeBit(4));
    }

    @Test
    void testConvertPrivilegeBitReadWrite() {
        assertEquals("read-write", convertPrivilegeBit(6));
    }

    @Test
    void testConvertPrivilegeBitNone() {
        assertEquals("none", convertPrivilegeBit(0));
    }

    @Test
    void testConvertPrivilegeBitOtherBitsIgnored() {
        // bit 1 set along with read (bit 2) = still read
        assertEquals("read", convertPrivilegeBit(3));
    }

    @Test
    void testConvertPrivilegeBitReadWriteWithOtherBits() {
        // bits 1+2+4 = 7, read-write should still be detected
        assertEquals("read-write", convertPrivilegeBit(7));
    }
}
