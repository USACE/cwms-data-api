/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda;

import fixtures.CwmsDataApiSetupCallback;
import fixtures.IntegrationTestNameGenerator;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.LauncherSessionListener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntegrationTestNameGeneratorTest {

    @Test
    void testGenerateDisplayNames() throws NoSuchMethodException {
        IntegrationTestNameGenerator generator = new IntegrationTestNameGenerator();
        String expectedSuffix = " (schema: " + CwmsDataApiSetupCallback.getSchemaVersionString() + ")";

        assertEquals(
            IntegrationTestNameGeneratorTest.class.getSimpleName() + expectedSuffix,
            generator.generateDisplayNameForClass(IntegrationTestNameGeneratorTest.class)
        );

        assertEquals(
            NestedClass.class.getSimpleName() + expectedSuffix,
            generator.generateDisplayNameForNestedClass(NestedClass.class)
        );

        Method testMethod = IntegrationTestNameGeneratorTest.class.getDeclaredMethod("testGenerateDisplayNames");
        assertEquals(
            IntegrationTestNameGeneratorTest.class.getSimpleName() + "." + testMethod.getName() + expectedSuffix,
            generator.generateDisplayNameForMethod(IntegrationTestNameGeneratorTest.class, testMethod)
        );
    }

    @Test
    void testLauncherSessionListenerRegistered() {
        ServiceLoader<LauncherSessionListener> loader = ServiceLoader.load(LauncherSessionListener.class);
        List<LauncherSessionListener> listeners = new ArrayList<>();
        loader.forEach(listeners::add);

        boolean found = listeners.stream()
            .anyMatch(l -> l instanceof CwmsDataApiSetupCallback);
        assertTrue(found, "CwmsDataApiSetupCallback should be registered as a LauncherSessionListener");
    }

    @Test
    void testParseVersionInt() {
        assertEquals(230316, CwmsDataApiSetupCallback.parseVersionInt("23.03.16"));
        assertEquals(250701, CwmsDataApiSetupCallback.parseVersionInt("25.07.01"));
        assertEquals(260217, CwmsDataApiSetupCallback.parseVersionInt("26.02.17-RC01"));
        assertEquals(1009999, CwmsDataApiSetupCallback.parseVersionInt("99.99.99.9-CDA_STAGING"));
        assertEquals(999999, CwmsDataApiSetupCallback.parseVersionInt("latest-dev"));
        assertEquals(-1, CwmsDataApiSetupCallback.parseVersionInt("Bypass"));
        assertEquals(-1, CwmsDataApiSetupCallback.parseVersionInt(null));
        assertEquals(-1, CwmsDataApiSetupCallback.parseVersionInt(""));
    }

    static class NestedClass {
    }
}
