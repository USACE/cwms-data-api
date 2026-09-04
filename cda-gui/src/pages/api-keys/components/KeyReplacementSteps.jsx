import { H3, Text } from "@usace/groundwork";

export default function KeyReplacementSteps() {
  return (
    <div className="space-y-5">
      <div>
        <H3>Replace a key</H3>
        <ol className="mt-3 list-decimal space-y-3 pl-5 text-zinc-700 marker:font-bold">
          <li>
            Select the old key and choose <strong>Rotate key</strong>.
          </li>
          <li>
            Choose a new name and expiration date, then select{" "}
            <strong>Generate replacement</strong>.
          </li>
          <li>Copy the new key and save it securely. You can only see it once.</li>
          <li>
            Update your application to use the new key and check that its requests work.
          </li>
          <li>
            Select <strong>Close</strong>, then confirm revocation of the old key. If
            you are not ready, cancel the confirmation to keep both keys.
          </li>
        </ol>
      </div>
      <div>
        <H3>Revoke a key without replacing it</H3>
        <ol className="mt-3 list-decimal space-y-3 pl-5 text-zinc-700 marker:font-bold">
          <li>Select the key you want to remove.</li>
          <li>
            Choose <strong>Revoke key</strong>.
          </li>
          <li>Check the key name in the confirmation, then confirm revocation.</li>
        </ol>
      </div>
      <Text>
        <strong>Revocation is permanent.</strong> Any application still using the old
        key will lose access.
      </Text>
      <Text>
        Expired keys are marked in red and cannot authenticate requests. Follow the
        replacement steps to get a usable key, or revoke the expired key to remove it.
      </Text>
    </div>
  );
}
