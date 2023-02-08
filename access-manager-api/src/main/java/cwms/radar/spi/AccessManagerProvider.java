package cwms.radar.spi;

public interface AccessManagerProvider {
    public String getName();
    public RadarAccessManager create();
}
