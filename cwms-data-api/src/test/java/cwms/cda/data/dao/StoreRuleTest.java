package cwms.cda.data.dao;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StoreRuleTest {

    @Test
    public void testGetStoreRule() {
        StoreRule rule = StoreRule.getStoreRule("REPLACE_ALL");
        assertNotNull(rule);
        assertEquals(StoreRule.REPLACE_ALL, rule);
        assertEquals("REPLACE ALL", rule.getRule());
    }

    @Test
    public void testGetStoreRuleSpace() {
        StoreRule rule = StoreRule.getStoreRule("REPLACE ALL");
        assertNotNull(rule);
        assertEquals(StoreRule.REPLACE_ALL, rule);
        assertEquals("REPLACE ALL", rule.getRule());
    }

    @Test
    public void testToStringSpaces() {
        String replaceAllStr = StoreRule.REPLACE_ALL.toString();

        assertEquals("REPLACE ALL", replaceAllStr);
    }

}