import org.gradle.api.provider.ValueSource
import org.gradle.api.provider.ValueSourceParameters
import org.gradle.process.ExecOperations

import javax.inject.Inject

/**
 * Resolves the Docker daemon's server version, or an empty string if Docker is not
 * installed or the daemon is unreachable.
 *
 * Gradle guarantees a {@link ValueSource}'s {@code obtain()} is invoked at most once
 * per build, so this replaces hand-rolled caching of the "is Docker available" check.
 */
abstract class DockerVersionValueSource implements ValueSource<String, ValueSourceParameters.None> {

    @Inject
    abstract ExecOperations getExecOperations()

    @Override
    String obtain() {
        try {
            def stdout = new ByteArrayOutputStream()

            execOperations.exec {
                commandLine 'docker', 'info', '--format', '{{.ServerVersion}}'
                standardOutput = stdout
                errorOutput = new ByteArrayOutputStream()
                ignoreExitValue = true
            }

            return stdout.toString().trim()
        } catch (ignored) {
            return ''
        }
    }
}
