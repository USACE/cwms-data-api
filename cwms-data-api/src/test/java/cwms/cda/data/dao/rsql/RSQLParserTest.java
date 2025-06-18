package cwms.cda.data.dao.rsql;

import cz.jirutka.rsql.parser.ast.Node;
import cz.jirutka.rsql.parser.RSQLParser;
import org.jooq.Condition;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RSQLParserTest {

    @Test
    void testParse(){
        Node root = new RSQLParser().parse("unit_id==EN;value>25");

        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        Condition testCondition = root.accept(new RSQLJooqConditionVisitor(resolver));
        assertNotNull(testCondition);
        String condStr = testCondition.toString();
        assertTrue( condStr.contains("\"UNIT_ID\" = 'EN'"));

    }

    @Test
    void testParseQuote(){
        Node root = new RSQLParser().parse("unit_id==\"EN\";value>25");

        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        Condition testCondition = root.accept(new RSQLJooqConditionVisitor(resolver));
        assertNotNull(testCondition);
        String condStr = testCondition.toString();
        assertTrue( condStr.contains("\"UNIT_ID\" = 'EN'"));

    }

    @Test
    void testParseNull(){
        Node root = new RSQLParser().parse("unit_id==EN;value!=null");

        JooqFieldResolver resolver = new JooqFieldResolver(AV_TSV_DQU.AV_TSV_DQU);

        Condition testCondition = root.accept(new RSQLJooqConditionVisitor(resolver));
        assertNotNull(testCondition);
        String condStr = testCondition.toString();
        System.out.println( condStr );
        assertTrue( condStr.contains("\"UNIT_ID\" = 'EN'"));
    }
}
