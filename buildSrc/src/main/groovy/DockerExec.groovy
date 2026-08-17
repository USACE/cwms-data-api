import org.gradle.api.provider.ProviderFactory
import org.gradle.api.tasks.Exec

import javax.inject.Inject

/**
 * An {@link Exec} task that is automatically skipped (with a lifecycle message) when
 * Docker is not installed or the Docker daemon is unavailable, instead of failing the build.
 */
abstract class DockerExec extends Exec {

    @Inject
    abstract ProviderFactory getProviders()

    DockerExec() {
        onlyIf { isDockerAvailable() }
    }

    private boolean isDockerAvailable() {
        def version = providers.of(DockerVersionValueSource) {}.get()

        if (!version) {
            logger.lifecycle("Skipping ${name} because Docker is not installed or the Docker daemon is unavailable.")
            return false
        }

        logger.info("Docker is available: ${version}")
        return true
    }
}
