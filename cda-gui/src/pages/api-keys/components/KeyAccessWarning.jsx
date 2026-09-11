import PropTypes from "prop-types";
import { Button, UsaceBox } from "@usace/groundwork";
import { FaExclamationTriangle } from "react-icons/fa";

export default function KeyAccessWarning({ missingRoles, loading, retry, feedback }) {
  return (
    <section className="pb-12">
      <UsaceBox title="API Keys">
        <div className="mx-auto max-w-3xl py-6 sm:py-10">
          <div
            className="rounded-lg border border-amber-300 bg-amber-50 p-6 text-amber-950"
            role="alert"
          >
            <FaExclamationTriangle
              aria-hidden="true"
              className="mb-4 text-3xl text-amber-700"
            />
            <h1 className="mb-3 text-2xl font-bold">API key access required</h1>
            <p className="mb-4">
              Your account or current sign-in does not have access to create or manage
              API keys. Both of these are required:
            </p>
            <ul className="list-disc space-y-3 pl-5">
              <li>
                <strong>CWMS Users</strong> permission
                {missingRoles.includes("CWMS Users") && (
                  <strong> — missing from your current access</strong>
                )}
                .
              </li>
              <li>
                A signed-in user session (<code>cac_auth</code>)
                {missingRoles.includes("cac_auth") && (
                  <strong> — missing from your current sign-in</strong>
                )}
                . Sign in with your user account; an API key cannot manage other keys.
              </li>
            </ul>
            <p className="mt-5 font-semibold">
              Reach out to your CWMS Admin to request the required permission and verify
              your sign-in access.
            </p>
            <p className="mt-3">
              After your access is updated, sign out and sign in again, then check
              access below.
            </p>
          </div>
          <div className="mt-5 flex flex-wrap gap-3">
            <Button type="button" disabled={loading} onClick={retry}>
              {loading ? "Checking access…" : "Check access again"}
            </Button>
            <Button color="light" href="/api-keys/help">
              How to use keys
            </Button>
          </div>
        </div>
      </UsaceBox>
      {feedback}
    </section>
  );
}

KeyAccessWarning.propTypes = {
  missingRoles: PropTypes.arrayOf(PropTypes.string).isRequired,
  loading: PropTypes.bool.isRequired,
  retry: PropTypes.func.isRequired,
  feedback: PropTypes.node,
};
