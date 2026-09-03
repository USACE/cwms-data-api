import { useSearchParams } from "react-router-dom";
import { useEffect } from "react";
import { useAuth } from "@usace-watermanagement/groundwork-water";
import { Badge, Button, H1, Text } from "@usace/groundwork";
import KeyHelpStep from "./KeyHelpStep";
import KeyUsageExample from "./KeyUsageExample";
import KeyHelpTroubleshooting from "./KeyHelpTroubleshooting";

export default function KeyHelpGuide() {
  useEffect(() => {
    window.scrollTo(0, 0);
  }, []);
  const [params] = useSearchParams();
  const { profile } = useAuth();
  const offices = Object.keys(profile?.roles ?? {}).sort();
  const requested = params.get("office");
  const office = offices.includes(requested) ? requested : (offices[0] ?? "");
  const returnOffice = requested ?? office;
  const backLink = `/api-keys${returnOffice ? `?office=${encodeURIComponent(returnOffice)}` : ""}`;

  return (
    <section className="mx-auto max-w-4xl pb-12">
      <div className="mb-6 border-b border-zinc-200 pb-6">
        <Button href={backLink} color="light" className="mb-5">
          Back to API Keys
        </Button>
        <div className="mb-3">
          <Badge color="blue">API Keys · Guide</Badge>
        </div>
        <H1>How to use API keys</H1>
        <Text className="mt-3">Connect a script or application in four steps.</Text>
      </div>
      <div className="mb-6 rounded-lg border border-blue-200 bg-blue-50 p-5">
        <Text>
          <strong>Your key acts as you.</strong> It belongs to your user account and
          uses your existing office permissions. Choosing an office on the API Keys page
          does not restrict the key to that office.
        </Text>
      </div>
      <ol className="list-none space-y-5 p-0">
        <KeyHelpStep number={1} title="Create a key">
          <Text>
            Open <strong>API Keys → Create key</strong>. Give each application its own
            unique name, such as <code>daily-report</code>.
          </Text>
          <Text>
            Choose an expiration date. The key expires at the start of that date in UTC;
            a blank date means no expiration.
          </Text>
        </KeyHelpStep>
        <KeyHelpStep number={2} title="Save the secret once">
          <Text>
            Copy the generated key into your application&apos;s secure secret store
            before closing the dialog. CDA cannot show it again.
          </Text>
          <ul className="list-disc space-y-2 pl-5 text-zinc-700">
            <li>Keep the key private. Do not share it with coworkers.</li>
            <li>Keep it out of URLs, source code, and public browser apps.</li>
            <li>If you lose it, create a replacement and revoke the old key.</li>
          </ul>
        </KeyHelpStep>
        <KeyHelpStep number={3} title="Send an authenticated request">
          <KeyUsageExample office={office} />
        </KeyHelpStep>
        <KeyHelpStep number={4} title="Replace or revoke the key">
          <Text>
            Before expiration, create a replacement and update your application. Then
            select the old key and choose <strong>Revoke key</strong>.
          </Text>
          <Text>
            Revocation is permanent. Any application still using the old key will lose
            access.
          </Text>
        </KeyHelpStep>
      </ol>
      <div className="mt-6">
        <KeyHelpTroubleshooting />
      </div>
      <div className="mt-6">
        <Button href={backLink}>Back to API Keys</Button>
      </div>
    </section>
  );
}
