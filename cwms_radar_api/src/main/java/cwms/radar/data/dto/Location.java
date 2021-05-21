package cwms.radar.data.dto;

import java.sql.ResultSet;
import java.sql.SQLException;


 public class Location implements CwmsDTO{
    private String location_id;
    private double latitude;
    private double longitude;

    public Location(){}

    public Location(ResultSet rs) throws SQLException {
        location_id = rs.getString("location_id");
        latitude = rs.getDouble("latitude");
        longitude = rs.getDouble("longitude");
    }

    public String getLocationId(){ return location_id; }
    public double getLatitude(){ return latitude; }
    public double getLongitude(){ return longitude; }

 }