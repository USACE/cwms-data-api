import { Card, H2, Strong, Text } from "@usace/groundwork";

export default function KeyHelpTroubleshooting() {
  return (
    <Card className="p-5 sm:p-7">
      <H2 className="mb-4 text-xl">If a request fails</H2>
      <dl className="space-y-4">
        <div>
          <dt>
            <Strong>Your sign-in could not be verified</Strong>
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
            <Strong>Access was denied</Strong>
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
            <Text>Sign in. An API key cannot create, list, or revoke keys.</Text>
          </dd>
        </div>
      </dl>
      <div className="mt-4">
        <Strong>CDA could not complete the request</Strong>
        <Text>
          Refresh your keys to check whether the change was saved before trying again.
          Contact your office administrator if the problem continues.
        </Text>
      </div>
    </Card>
  );
}
