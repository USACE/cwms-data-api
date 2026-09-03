import PropTypes from "prop-types";
import { Strong, Text } from "@usace/groundwork";

export default function KeyUsageExample({ office }) {
  const endpoint = new URL(
    `${import.meta.env.VITE_CDA_API_ROOT.replace(/\/$/, "")}/timeseries`,
    window.location.origin,
  ).href;
  const example = [
    "curl --get \\",
    '  --header "Authorization: apikey $CWMS_API_KEY" \\',
    '  --header "Accept: application/json;version=2" \\',
    `  --data-urlencode "office=${office || "YOUR_OFFICE"}" \\`,
    '  --data-urlencode "name=YOUR_TIMESERIES" \\',
    `  "${endpoint}"`,
  ].join("\n");

  return (
    <div className="space-y-3">
      <Text>
        Send the key in this header. Include the space after <code>apikey</code>.
      </Text>
      <pre className="whitespace-pre-wrap break-all rounded-lg bg-blue-50 p-4 text-sm">
        <code>Authorization: apikey YOUR_KEY</code>
      </pre>
      <div>
        <Strong>Try a request (Bash / curl)</Strong>
      </div>
      <Text>
        Load your saved key into the <code>CWMS_API_KEY</code> environment variable.
        Replace <code>YOUR_TIMESERIES</code> with a valid time-series ID.
      </Text>
      <pre
        aria-label="curl example"
        className="whitespace-pre-wrap break-all rounded-lg bg-zinc-100 p-4 text-sm leading-6"
      >
        <code>{example}</code>
      </pre>
      <Text>
        The example uses {office || "YOUR_OFFICE"}. Use the office required by the
        endpoint you are calling.
      </Text>
    </div>
  );
}

KeyUsageExample.propTypes = { office: PropTypes.string.isRequired };
