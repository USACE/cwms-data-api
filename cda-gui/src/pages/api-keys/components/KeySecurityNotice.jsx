import { FaExclamationTriangle } from "react-icons/fa";

export default function KeySecurityNotice() {
  return (
    <aside
      aria-label="API key security update"
      className="mb-6 flex gap-3 rounded-lg border-l-4 border-amber-500 bg-amber-50 p-5 text-amber-950"
    >
      <FaExclamationTriangle
        aria-hidden="true"
        className="mt-1 h-6 w-6 shrink-0 text-amber-600"
      />
      <div>
        <p className="text-lg font-bold">
          Regenerate keys after the May 2026 security update
        </p>
        <p className="mt-2">
          When upgrading to CDA 2026.05.12 or later, replace keys created before the
          security update. Older keys are no longer accepted by default. Sign in,
          generate a new key, update your applications, and revoke the old key. Follow
          the steps below to complete the replacement.
        </p>
      </div>
    </aside>
  );
}
