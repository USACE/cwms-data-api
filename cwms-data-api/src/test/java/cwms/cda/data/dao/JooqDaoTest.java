/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao;

import static org.jooq.SQLDialect.ORACLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.SQLException;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

final class JooqDaoTest {
    private DSLContext dsl;
    private Field<String> locationLevelId;

    @BeforeEach
    void setUp() {
        dsl = DSL.using(SQLDialect.ORACLE);
        locationLevelId = DSL.field(DSL.name("LOCATION_LEVEL_ID"), String.class);
    }

    @Nested
    @DisplayName("No-op patterns")
    class NoOpPatterns {

        @Test
        @DisplayName("single asterisk renders no condition")
        void singleAsteriskRendersNoCondition() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "*");
            String sql = render(condition);
            assertTrue(sqlEqualsNoCondition(sql), sql);
        }

        @Test
        @DisplayName("dot star renders no condition")
        void dotStarRendersNoCondition() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, ".*");
            String sql = render(condition);
            assertTrue(sqlEqualsNoCondition(sql), sql);
        }
    }

    @Nested
    @DisplayName("Plain text patterns")
    class PlainTextPatterns {

        @Test
        @DisplayName("plain text renders case-insensitive equality")
        void plainTextRendersCaseInsensitiveEquality() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "Top of Dam");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "upper(\"LOCATION_LEVEL_ID\") = ?");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
            assertFalse(sql.toLowerCase().contains(" like "), sql);
        }

        @Test
        @DisplayName("plain text with dots is treated as literal text")
        void plainTextWithDotsIsTreatedAsLiteralText() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "APP.Stor.Inst.0.Top of Dam");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "upper(\"LOCATION_LEVEL_ID\") = ?");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
            assertFalse(sql.toLowerCase().contains(" like "), sql);
        }

        @Test
        @DisplayName("plain text value is bound, not inlined")
        void plainTextValueIsBoundNotInlined() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "APP.Stor.Inst.0.Top of Dam");

            String sql = render(condition);

            assertTrue(sql.contains("?"), sql);
            assertFalse(sql.contains("APP.STOR.INST.0.TOP OF DAM"), sql);
        }

        @Test
        @DisplayName("plain text comparison uppercases the value")
        void plainTextComparisonUppercasesValue() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "App.Stor.Inst.0.Top of Dam");

            String sql = renderInlined(condition);

            assertTrue(sql.contains("'APP.STOR.INST.0.TOP OF DAM'"), sql);
        }
    }

    @Nested
    @DisplayName("Glob patterns")
    class GlobPatterns {

        @Test
        @DisplayName("trailing asterisk renders LIKE")
        void trailingAsteriskRendersLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.Stor.*");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "upper(\"LOCATION_LEVEL_ID\") like ? escape '\\'");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("asterisk in middle renders LIKE")
        void asteriskInMiddleRendersLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.*.Inst");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "upper(\"LOCATION_LEVEL_ID\") like ? escape '\\'");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("multiple asterisks render LIKE")
        void multipleAsterisksRenderLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.*.Inst.*");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "upper(\"LOCATION_LEVEL_ID\") like ? escape '\\'");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("glob pattern converts asterisk to percent")
        void globPatternConvertsAsteriskToPercent() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.Stor.*");
            String sql = renderInlined(condition);
            assertTrue(sql.contains("'APP.STOR%'"), sql);
        }

        @Test
        @DisplayName("glob pattern escapes SQL LIKE percent")
        void globPatternEscapesSqlLikePercent() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.100%.*");
            String sql = renderInlined(condition);
            assertTrue(sql.contains("'APP.100\\%%'"), sql);
        }

        @Test
        @DisplayName("glob pattern escapes SQL LIKE underscore")
        void globPatternEscapesSqlLikeUnderscore() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.Some_Level.*");
            String sql = renderInlined(condition);
            assertTrue(sql.contains("'APP.SOME\\_LEVEL%'"), sql);
        }

        @Test
        @DisplayName("glob pattern with backslash uses regex_like")
        void globPatternEscapesBackslash() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.Some\\Level.*");
            String sql = renderInlined(condition);
            assertTrue(sql.contains("'APP.Some\\Level.*'"), sql);
            assertTrue(sql.contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("glob value is bound, not inlined")
        void globValueIsBoundNotInlined() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.Stor.*");
            String sql = render(condition);
            assertTrue(sql.contains("?"), sql);
            assertFalse(sql.contains("APP.STOR.%"), sql);
        }
    }

    @Nested
    @DisplayName("Explicit regex patterns")
    class ExplicitRegexPatterns {

        @Test
        @DisplayName("REGEX prefix renders Oracle regexp_like")
        void regexPrefixRendersRegexpLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "^APP\\.Stor\\..*");

            String sql = render(condition);

            assertTrue(sql.toLowerCase().contains("regexp_like"), sql);
            assertTrue(sql.contains("\"LOCATION_LEVEL_ID\""), sql);
            assertTrue(sql.contains("'i'"), sql);
        }

        @Test
        @DisplayName("REGEX prefix is case-insensitive")
        void regexPrefixIsCaseInsensitive() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "^APP\\.Stor\\..*");

            String sql = render(condition);

            assertTrue(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("regex value is inlined, not bound")
        void regexValueIsInlinedNotBound() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "^APP\\.Stor\\..*");

            String sql = render(condition);

            assertFalse(sql.contains("?"), sql);
            assertTrue(sql.contains("'^APP\\.Stor\\..*'"), sql);
        }

        @Test
        @DisplayName("regex preserves regex dots and wildcards")
        void regexPreservesRegexSyntax() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "^APP.Stor.*Dam$");

            String sql = render(condition);

            assertTrue(sql.contains("'^APP.Stor.*Dam$'"), sql);
        }
    }

    @Nested
    @DisplayName("Negated patterns")
    class NegatedPatterns {

        @Test
        @DisplayName("NOT plain text renders negated equality")
        void notPlainTextRendersNegatedEquality() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "NOT:Top of Dam");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "not (upper(\"LOCATION_LEVEL_ID\") = ?)");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("NOT plain text with dots renders negated equality")
        void notPlainTextWithDotsRendersNegatedEquality() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "NOT:APP.Stor.Inst.0.Top of Dam");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "not (upper(\"LOCATION_LEVEL_ID\") = ?)");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("NOT glob renders negated LIKE")
        void notGlobRendersNegatedLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "NOT:APP.Stor.*");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(sql, "not (upper(\"LOCATION_LEVEL_ID\") like ? escape '\\')");
            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("NOT REGEX renders negated regexp_like")
        void notRegexRendersNegatedRegexpLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "NOT:^APP\\.Stor\\..*");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(
                sql,
                "not (regexp_like(\"LOCATION_LEVEL_ID\", '^APP\\.Stor\\..*', 'i'))");
        }

        @Test
        @DisplayName("NOT REGEX prefix is case-insensitive")
        void notRegexPrefixIsCaseInsensitive() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "NOT:^APP\\.Stor\\..*");

            String sql = render(condition);

            assertTrue(sql.toLowerCase().contains("regexp_like"), sql);
            assertTrue(sql.toLowerCase().startsWith("not"), sql);
        }

        @Test
        @DisplayName("NOT prefix is case-insensitive")
        void notPrefixIsCaseInsensitive() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "not:APP.Stor.*");

            String sql = render(condition);

            assertTrue(sql.toLowerCase().startsWith("not"), sql);
            assertContainsIgnoringWhitespace(sql, "upper(\"LOCATION_LEVEL_ID\") like ? escape '\\'");
        }
    }

    @Nested
    @DisplayName("Oracle rendering")
    class OracleRendering {

        @Test
        @DisplayName("explicit regex renders Oracle regexp_like with i flag")
        void explicitRegexRendersOracleRegexpLikeWithIFlag() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(
                locationLevelId,
                "^APP\\.Stor\\..*");

            String sql = render(condition);

            assertContainsIgnoringWhitespace(
                sql,
                "(regexp_like(\"LOCATION_LEVEL_ID\", '^APP\\.Stor\\..*', 'i'))");
        }

        @Test
        @DisplayName("plain text does not render Oracle regexp_like")
        void plainTextDoesNotRenderOracleRegexpLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.Stor");

            String sql = render(condition);

            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }

        @Test
        @DisplayName("glob does not render Oracle regexp_like")
        void globDoesNotRenderOracleRegexpLike() {
            Condition condition = JooqDao.caseInsensitiveLikeRegex(locationLevelId, "APP.Stor.*");

            String sql = render(condition);

            assertFalse(sql.toLowerCase().contains("regexp_like"), sql);
        }
    }

    private String render(Condition condition) {
        return dsl.render(condition);
    }

    private String renderInlined(Condition condition) {
        return dsl.renderInlined(condition);
    }

    private static void assertContainsIgnoringWhitespace(String actual, String expected) {
        String normalizedActual = normalizeWhitespace(actual);
        String normalizedExpected = normalizeWhitespace(expected);

        assertTrue(
            normalizedActual.contains(normalizedExpected),
            () -> "Expected SQL to contain:\n"
                + normalizedExpected
                + "\nActual SQL:\n"
                + normalizedActual);
    }

    private static boolean sqlEqualsNoCondition(String sql) {
        String normalized = normalizeWhitespace(sql);
        return "true".equalsIgnoreCase(normalized)
            || "1 = 1".equalsIgnoreCase(normalized)
            || "1=1".equalsIgnoreCase(normalized);
    }

    private static String normalizeWhitespace(String value) {
        return value.replaceAll("\\s+", " ").trim();
    }

    @Test
    void testCaseInsensitiveLikeRegexNullTrue() {
        Field<String> field = DSL.field("my_field", String.class);
        Condition cond = JooqDao.caseInsensitiveLikeRegexNullTrue(field, null);
        assertEquals("1 = 1", DSL.using(ORACLE).renderInlined(cond));
    }

    @Test
    void testFormatBool() {
        assertEquals("T", Dao.formatBool(true));
        assertEquals("F", Dao.formatBool(false));
        assertNull(Dao.formatBool(null));
    }

    @Test
    void testParseBool() {
        assertTrue(JooqDao.parseBool("T"));
        assertFalse(JooqDao.parseBool("F"));
        assertFalse(JooqDao.parseBool("ABC"));
        assertFalse(JooqDao.parseBool(null));
    }

    @Test
    void testToBigDecimal() {
        assertEquals(BigDecimal.valueOf(5.5), JooqDao.toBigDecimal(5.5));
        assertNull(JooqDao.toBigDecimal(null));
    }

    @Test
    void testBuildDouble() {
        assertEquals(5.5, JooqDao.buildDouble(BigDecimal.valueOf(5.5)), 0.0);
        assertEquals(0.0, JooqDao.buildDouble(null));
    }

    @Test
    void testInvalidOfficeId() {
        String msg = "ORA-20010: INVALID_OFFICE_ID: \"NWDW\" is not a valid CWMS office id";
        SQLException ex = new SQLException(msg, "", 20010);
        assertTrue(JooqDao.isInvalidOffice(new DataAccessException(msg, ex)));
    }
}
