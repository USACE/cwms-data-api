package cwms.radar.data.dao;

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

}