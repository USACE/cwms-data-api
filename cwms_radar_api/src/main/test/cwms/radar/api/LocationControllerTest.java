package cwms.radar.api;

import io.javalin.http.Context;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 */
public class LocationControllerTest {
    
    private Context ctx = mock(Context.class);
    private Connection conn = mock(Connection.class);
    private PreparedStatement query = mock(PreparedStatement.class);
    private ResultSet results = mock(ResultSet.class);
    
    public LocationControllerTest() {
    }
    
    @BeforeAll
    public static void setUpClass() {
    }
    
    @AfterAll
    public static void tearDownClass() {
    }
    
    @BeforeEach
    public void setUp() {
    }
    
    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of getAll method, of class LocationController.
     */
    @Test
    public void testGetAll() throws SQLException {
        System.out.println("getAll");        
        LocationController instance = new LocationController();
                
        when( ctx.attribute("db")).thenReturn(conn);
        when( conn.prepareStatement(LocationController.ALL_LOCATIONS_QUERY)).thenReturn(query);
        when( query.executeQuery() ).thenReturn(results);
        when( results.next() ).thenReturn(true).thenReturn(false);
        when( results.getString("location_id")).thenReturn("test");
        when( results.getString("latitude")).thenReturn(0);
        when( results.getSTring("longitude")).thenReturn(1);
        instance.getAll(ctx);
        
        verify(ctx).status(200);
        
    }

    /**
     * Test of getOne method, of class LocationController.
     */
    @Test
    public void testGetOne() throws SQLException {
        System.out.println("getOne");        
        String Location_id = "1";
        LocationController instance = new LocationController();
        
        when( ctx.attribute("db")).thenReturn(conn);
        when( conn.prepareStatement(LocationController.SINGLE_Location_QUERY)).thenReturn(query);        
        when( query.executeQuery() ).thenReturn(results);
        when( results.next() ).thenReturn(true).thenReturn(false);
        when( results.getString("Locationname")).thenReturn("test");
        
        
        
        instance.getOne(ctx, Location_id);
        verify(ctx).status(200);
        
    }

    /**
     * Test of create method, of class LocationController.
     */
    @Test
    public void testCreate() {
        System.out.println("create");        
        LocationController instance = new LocationController();
        Assertions.assertThrows( UnsupportedOperationException.class , () -> {
            instance.create(ctx);
        });
        
        
    }

    /**
     * Test of update method, of class LocationController.
     */
    @Test
    public void testUpdate() {
        System.out.println("update");        
        String Location_id = "";
        LocationController instance = new LocationController();
        Assertions.assertThrows( UnsupportedOperationException.class, () -> {
            instance.update(ctx, Location_id);
        });
        
      
    }

    /**
     * Test of delete method, of class LocationController.
     */
    @Test
    public void testDelete() {
        System.out.println("delete");        
        String Location_id = "";
        LocationController instance = new LocationController();
        Assertions.assertThrows( UnsupportedOperationException.class, () -> {
            instance.delete(ctx, Location_id);
        });
        
        
    }
    
}
