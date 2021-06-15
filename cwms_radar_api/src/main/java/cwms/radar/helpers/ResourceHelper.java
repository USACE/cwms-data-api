package cwms.radar.helpers;

import java.io.IOException;
import java.io.InputStream;

import org.eclipse.jetty.util.IO;

public final class ResourceHelper {
    /**
     * Returns resource as a string, null if resource can't be found.
     * @param resource The path to the resource
     * @param context The class context to load the resource from
     * @return The contents of the resource
     * */
    public static String getResourceAsString(String resource, Class<?> context) {
        InputStream formatList = getResourceAsStream(resource, context);
        try {
            return IO.toString(formatList);
        } catch (IOException e) {
            e.printStackTrace();
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
