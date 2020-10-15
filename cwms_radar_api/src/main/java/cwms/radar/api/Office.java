/*
 * The MIT License 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package cwms.radar.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class Office {
    private String name;
    private String long_name;
    private String type;
    private String reports_to;

    private final HashMap<String,String> office_types = new HashMap<>(){
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