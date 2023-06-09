package cwms.cda.spi;

public interface AccessManagerProvider {
    public String getName();
    public CdaAccessManager create();
}
