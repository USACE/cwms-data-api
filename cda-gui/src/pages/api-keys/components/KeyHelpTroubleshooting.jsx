import { Card, H2, Strong, Text } from "@usace/groundwork";

export default function KeyHelpTroubleshooting() {
  return (
    <Card className="p-5 sm:p-7">
      <H2 className="mb-4 text-xl">If a request fails</H2>
      <dl className="space-y-4">
        <div>
          <dt>
            <Strong>401 — Check the credential</Strong>
          </dt>
          <dd>
            <Text>
              Confirm the header prefix and key value. The key may be invalid, expired,
              or revoked.
            </Text>
          </dd>
        </div>
        <div>
          <dt>
            <Strong>403 — Check your access</Strong>
          </dt>
          <dd>
            <Text>
              Check the requested office and your user permissions. Contact your office
              administrator if access is missing.
            </Text>
          </dd>
        </div>
        <div>
          <dt>
            <Strong>Cannot manage keys?</Strong>
          </dt>
          <dd>
            <Text>
              Sign in interactively. An API key cannot create, list, or revoke keys.
            </Text>
          </dd>
        </div>
      </dl>
    </Card>
  );
}
