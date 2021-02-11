package cwms.radar.data.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class Office {
    private String name;
    private String long_name;
    private String type;
    private String reports_to;

    private final HashMap<String,String> office_types = new HashMap<String,String>(){
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        {
            put("UNK","unknown");
            put("HQ","corps headquarters");
            put("MSC","division headquarters");
            put("MSCR","division regional");
            put("DIS","district");
            put("FOA","field operating activity");
        }
    };

    public Office(ResultSet rs ) throws SQLException {
        name = rs.getString("office_id");
        long_name = rs.getString("long_name");
        type = office_types.get(rs.getString("office_type"));
        reports_to = rs.getString("report_to_office_id");
    }

    public String getName(){ return name; }
    public String getLong_Name(){ return long_name; }
    public String getType(){ return type; }
    public String getReports_To(){ return reports_to; }
}