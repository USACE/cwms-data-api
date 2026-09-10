import org.gradle.api.logging.Logger
import org.gradle.api.logging.Logging
import org.gradle.api.provider.ValueSource
import org.gradle.api.provider.ValueSourceParameters
import org.gradle.process.ExecOperations
import org.gradle.process.ExecSpec
import org.gradle.process.internal.ExecException

import javax.inject.Inject

/**
 * Resolves the Docker daemon's server version, or an empty string if Docker is not
 * installed or the daemon is unreachable.
 *
 * Gradle guarantees a {@link ValueSource}'s {@code obtain()} is invoked at most once
 * per build, so this replaces hand-rolled caching of the "is Docker available" check.
 */
abstract class DockerVersionValueSource implements ValueSource<String, ValueSourceParameters.None> {

    private static final Logger LOGGER = Logging.getLogger(DockerVersionValueSource)

    @Inject
    abstract ExecOperations getExecOperations()

    @Override
    String obtain() {
        def stdout = new ByteArrayOutputStream()
        def stderr = new ByteArrayOutputStream()

        try {
            execOperations.exec { ExecSpec spec ->
                spec.commandLine 'docker', 'info', '--format', '{{.ServerVersion}}'
                spec.standardOutput = stdout
                spec.errorOutput = stderr
                spec.ignoreExitValue = true
            }
        } catch (ExecException e) {
            return ''
        }

        def version = stdout.toString().trim()

        if (!version) {
            def error = stderr.toString().trim()
            LOGGER.info("'docker info' produced no version output.{}", error ? " stderr: ${error}" : '')
        }

        return version
    }
}
