package fixtures;

import java.util.Objects;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class FunctionalSchemaCondition implements ExecutionCondition {
private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult
        .enabled("@FunctionalSchema is not present");

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        final String schemaVersion = CwmsDataApiSetupCallback.VERSION_STRING;
        return context.getElement()
            .map(el -> el.getAnnotation(FunctionalSchemas.class))
            .filter(Objects::nonNull)
            .map(annotation -> {
                String[] schemas = annotation.values();
                
                
                if (!versionMatched(schemas, schemaVersion)) {
                    return ConditionEvaluationResult.disabled("Test disabled because schema version "
                        + schemaVersion + " is not in the target list of version");
                }
                return ConditionEvaluationResult.enabled("Test enabled because schema version "
                    + schemaVersion + " is in the target list of versions." );
            })
            .orElse(ENABLED);
    }


    private static boolean versionMatched(String[] versions, String activeVersion) {
        for (String version: versions) {
            if (version.equalsIgnoreCase(activeVersion)) {
                return true;
            }
        }
        return false;
    }
    
}
