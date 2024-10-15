/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

package fixtures;

import cwms.cda.data.dao.AuthDao;
import java.sql.SQLException;
import java.util.Objects;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class SchemaVersionCondition implements ExecutionCondition {

    private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult
        .enabled("@MinimumSchemaVersion is not present");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        return context.getElement()
            .map(el -> el.getAnnotation(MinimumSchema.class))
            .filter(Objects::nonNull)
            .map(annotation -> {
                int version = annotation.value();
                int currentVersion = getCurrentSchemaVersion();
                if (currentVersion < version) {
                    return ConditionEvaluationResult.disabled("Test disabled because schema version "
                        + currentVersion + " is less than " + version);
                }
                return ConditionEvaluationResult.enabled("Test enabled because schema version "
                    + currentVersion + " is at least " + version);
            })
            .orElse(ENABLED);
    }

    private int getCurrentSchemaVersion() {
        try {
            return CwmsDataApiSetupCallback.getDatabaseLink().connection(c->{
                return AuthDao.getInstance(DSL.using(c)).getDbVersion();
            }, CwmsDataApiSetupCallback.getWebUser());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}