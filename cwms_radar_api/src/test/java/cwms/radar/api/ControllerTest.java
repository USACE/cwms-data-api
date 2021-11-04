package cwms.radar.api;

import java.io.InputStream;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ControllerTest
{

    public String loadResourceAsString(String fileName)
    {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream stream = classLoader.getResourceAsStream(fileName);
        assertNotNull(stream, "Could not load the resource as stream:" + fileName);
        Scanner scanner = new Scanner(stream);
        String contents = scanner.useDelimiter("\\A").next();
        scanner.close();
        return contents;
    }
}
