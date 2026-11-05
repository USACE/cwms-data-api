import { Link, Strong, Text } from "@usace/groundwork";

export default function KeyUsageExample() {
  const apiRoot = new URL(
    `${import.meta.env.BASE_URL.replace(/\/$/, "")}/`,
    window.location.origin,
  ).href;

  return (
    <div className="space-y-3">
      <Text>
        The following examples show how to use the API key you created. You can test
        your key in Bash: on Windows, search for &quot;Git Bash&quot; if you have Git
        installed, or open a Bash terminal on Linux/Unix.
      </Text>
      <Text>
        Send the key in this header. Include the space after <code>apikey</code>.
      </Text>
      <pre className="whitespace-pre-wrap break-all rounded-lg bg-blue-50 p-4 text-sm">
        <code>Authorization: apikey YOUR_KEY</code>
      </pre>
      <div>
        <Strong>Set up your environment (Bash)</Strong>
      </div>
      <Text>
        Run these two export commands before either example below. Replace{" "}
        <code>YOUR_KEY</code> with your saved API key. These variables apply to your
        current terminal session.
      </Text>
      <pre
        aria-label="environment setup example"
        className="whitespace-pre-wrap break-all rounded-lg bg-zinc-100 p-4 text-sm leading-6"
      >
        <code className="language-bash">
          <span className="font-semibold text-blue-800">export</span>{" "}
          <span className="text-purple-800">CDA_API_KEY</span>=
          <span className="text-emerald-800">{'"YOUR_KEY"'}</span>
          {"\n"}
          <span className="font-semibold text-blue-800">export</span>{" "}
          <span className="text-purple-800">CDA_API_ROOT</span>=
          <span className="text-emerald-800">{`"${apiRoot}"`}</span>
        </code>
      </pre>
      <div>
        <Strong>List roles with curl (Bash)</Strong>
      </div>
      <Text>
        This request lists available roles using curl; cwms-cli is not required.
      </Text>
      <pre
        aria-label="curl example"
        className="whitespace-pre-wrap break-all rounded-lg bg-zinc-100 p-4 text-sm leading-6"
      >
        <code className="language-bash">
          <span className="font-semibold text-blue-800">curl</span>{" "}
          <span className="text-purple-800">--header</span>{" "}
          <span className="text-emerald-800">
            {'"Authorization: apikey '}
            <span className="text-purple-800">$CDA_API_KEY</span>
            {'"'}
          </span>
          {" \\\n  "}
          <span className="text-emerald-800">
            {'"'}
            <span className="text-purple-800">{"${CDA_API_ROOT}"}</span>
            {'roles"'}
          </span>
        </code>
      </pre>
      <div>
        <Strong>List roles with cwms-cli (Bash)</Strong>
      </div>
      <Text>
        If you have cwms-cli installed, use this equivalent command. It reads the
        environment variables you exported above.
      </Text>
      <pre
        aria-label="cwms-cli example"
        className="whitespace-pre-wrap break-all rounded-lg bg-zinc-100 p-4 text-sm leading-6"
      >
        <code className="language-bash">
          <span className="font-semibold text-blue-800">cwms-cli</span>
          {" users roles list-all"}
        </code>
      </pre>
      <Text>
        <Link
          className="text-blue-700 underline underline-offset-2"
          href="https://cwms-cli.readthedocs.io/en/latest/cli.html#cwms-cli-users-roles-list-all"
        >
          cwms-cli roles command documentation
        </Link>
      </Text>
    </div>
  );
}
