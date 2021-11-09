package cwms.radar.helpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class ResourceHelper {
    private static final Logger logger = Logger.getLogger(ResourceHelper.class.getName());
    /**
     * Returns resource as a string, null if resource can't be found.
     * @param resource The path to the resource
     * @param context The class context to load the resource from
     * @return The contents of the resource
     * */
    public static String getResourceAsString(String resource, Class<?> context) {
        InputStream formatList = getResourceAsStream(resource, context);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(formatList));) {
            reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            logger.log(Level.SEVERE,"Error access resource",e);
        }
        return null;
    }

        /**
     * Returns resource as a string, null if resource can't be found.
     * @param resource The path to the resource
     * @return The contents of the resource
     * */
    public static String getResourceAsString(String resource) {
        return getResourceAsString(resource, ResourceHelper.class);
    }

    /**
     * Returns resource as a stream, null if resource can't be found.
     * @param resource The path to the resource
     * @param context The class context to load the resource from
     * @return The contents of the resource
     * */
    public static InputStream getResourceAsStream(String resource, Class<?> context) {
        return context.getResourceAsStream(resource);
    }

    /**
     * Returns resource as a stream, null if resource can't be found.
     * @param resource The path to the resource
     * @return The contents of the resource
     * */
    public static InputStream getResourceAsStream(String resource) {
        return getResourceAsStream(resource, ResourceHelper.class);
    }
}
