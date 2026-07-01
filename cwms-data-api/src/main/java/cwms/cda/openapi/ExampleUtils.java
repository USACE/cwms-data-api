/*
 *
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.openapi;

import cwms.cda.ApiServlet;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.locationlevel.ConstantLocationLevel;
import cwms.cda.data.dto.locationlevel.LocationLevel;
import cwms.cda.data.dto.locationlevel.SeasonalLocationLevel;
import cwms.cda.data.dto.locationlevel.TimeSeriesLocationLevel;
import cwms.cda.data.dto.locationlevel.VirtualLocationLevel;
import cwms.cda.formatters.Formats;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.swagger.v3.oas.models.examples.Example;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;

public final class ExampleUtils {
    private ExampleUtils() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Method to add specific input examples to API endpoints as defaults.
     * Can be overridden with controller annotations.
     * Primarily for use with endpoints that accept multiple classes as input and cannot be represented via annotations.
     * Current annotation limitations are due to legacy Javalin library.
     *
     * @param ops the OpenApiOptions object to add the examples to.
     */
    public static void addEndpointExamples(OpenApiOptions ops) {
        String swaggerPath = "/swagger-docs";
        for (EndpointExamples endpoint : EndpointExamples.values()) {
            endpoint.getExamples().forEach(config ->
                ops.path(swaggerPath)
                    .addExample(config.targetClass, config.displayName,
                        buildExample(config.exampleClass, config.resourcePath))
            );
        }
    }

    /**
     * Builds an example object for the given class and resource path.
     * @param exampleClass the class of the example object
     * @param path the path to the example resource
     * @return Example object
     */
    private static Example buildExample(Class<? extends CwmsDTOBase> exampleClass, String path) {
        cwms.cda.formatters.ContentType contentType = Formats.parseHeader(Formats.JSON, exampleClass);
        Example example = new Example();
        try (InputStream stream = ApiServlet.class.getClassLoader().getResourceAsStream(path)) {
            if (stream == null) {
                throw new IllegalArgumentException("Unable to find example file: " + path);
            }
            String ex = IOUtils.toString(stream, StandardCharsets.UTF_8);
            ex = Formats.format(contentType, Formats.parseContent(contentType, ex, exampleClass));
            example.value(ex);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to load example file: " + path, ex);
        }
        return example;
    }

    // Enum to define example configurations by endpoint
    // Add examples here for other endpoints
    private enum EndpointExamples {
        LEVELS(Arrays.asList(
            new ExampleConfig(LocationLevel.class, "Constant Location Level",
                ConstantLocationLevel.class, "cwms/cda/data/levels/levels_constant_create.json"),
            new ExampleConfig(LocationLevel.class, "Seasonal Location Level",
                SeasonalLocationLevel.class, "cwms/cda/data/levels/levels_seasonal_create.json"),
            new ExampleConfig(LocationLevel.class, "Timeseries Location Level",
                TimeSeriesLocationLevel.class, "cwms/cda/data/levels/levels_timeseries_create.json"),
            new ExampleConfig(LocationLevel.class, "Virtual Location Level",
                VirtualLocationLevel.class, "cwms/cda/data/levels/levels_virtual_create.json")
        ));
        // Add more endpoints as needed

        private final List<ExampleConfig> examples;

        EndpointExamples(List<ExampleConfig> examples) {
            this.examples = examples;
        }

        public List<ExampleConfig> getExamples() {
            return examples;
        }
    }

    private static class ExampleConfig {
        final Class<? extends CwmsDTOBase> targetClass;
        final String displayName;
        final Class<? extends CwmsDTOBase> exampleClass;
        final String resourcePath;

        ExampleConfig(Class<? extends CwmsDTOBase> targetClass, String displayName,
            Class<? extends CwmsDTOBase> exampleClass, String resourcePath) {
            this.targetClass = targetClass;
            this.displayName = displayName;
            this.exampleClass = exampleClass;
            this.resourcePath = resourcePath;
        }
    }
}
