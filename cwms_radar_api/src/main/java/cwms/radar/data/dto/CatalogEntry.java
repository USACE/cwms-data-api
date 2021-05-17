package cwms.radar.data.dto;

public class CatalogEntry {
    private String location;
    private String parameter;
    private String dataType;
    private String interval;
    private String duration;
    private String version;

    public String getFullName(){
        StringBuilder builder = new StringBuilder();
        builder.append(location).append(".")
               .append(parameter).append(".")
               .append(dataType).append(".")
               .append(interval).append(".")
               .append(duration).append(".")
               .append(version);
        return builder.toString();
    }

    @Override
    public String toString(){
        return this.getFullName();
    }
}
