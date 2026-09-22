import { Link, Strong, Text } from "@usace/groundwork";

export default function KeyUsageExample() {
  const apiRoot = new URL(
    `${import.meta.env.BASE_URL.replace(/\/$/, "")}/`,
    window.location.origin,
  ).href;
  const example = [
    'curl --header "Authorization: apikey $CDA_API_KEY" \\',
    `  "${apiRoot}roles"`,
  ].join("\n");
  const cliExample = [
    `export CDA_API_ROOT="${apiRoot}"`,
    "cwms-cli users roles list-all",
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
        Export your saved key as <code>CDA_API_KEY</code>. These examples list available
        roles.
      </Text>
      <pre
        aria-label="curl example"
        className="whitespace-pre-wrap break-all rounded-lg bg-zinc-100 p-4 text-sm leading-6"
      >
        <code>{example}</code>
      </pre>
      <div>
        <Strong>Try a request (Bash / cwms-cli)</Strong>
      </div>
      <Text>
        Set <code>CDA_API_ROOT</code> to this CDA URL. cwms-cli also reads your exported{" "}
        <code>CDA_API_KEY</code>.
      </Text>
      <pre
        aria-label="cwms-cli example"
        className="whitespace-pre-wrap break-all rounded-lg bg-zinc-100 p-4 text-sm leading-6"
      >
        <code>{cliExample}</code>
      </pre>
      <Text>
        <Link href="https://cwms-cli.readthedocs.io/en/latest/cli.html#cwms-cli-users-roles-list-all">
          cwms-cli roles command documentation
        </Link>
      </Text>
    </div>
  );
}
