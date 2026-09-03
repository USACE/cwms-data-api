import PropTypes from "prop-types";
import { Card, Strong, Text, Field, Label, Description } from "@usace/groundwork";
export default function OfficeContext({ profile, office, offices, setOfficeChoice }) {
  return (
    <Card className="mb-6 p-5">
      <div className="grid gap-5 md:grid-cols-2">
        <div>
          <Strong>Signed in as</Strong>
          <Text>{profile?.userName ?? "Loading profile…"}</Text>
        </div>
        <Field>
          <Label htmlFor="key-office-context">Office context</Label>
          <select
            id="key-office-context"
            className="mt-2 block w-full rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm focus:ring-2 focus:ring-blue-600"
            aria-label="Office context"
            value={office}
            onChange={(event) => setOfficeChoice(event.target.value)}
            disabled={!offices.length}
          >
            {offices.length
              ? offices.map((id) => (
                  <option key={id} value={id}>
                    {id}
                  </option>
                ))
              : [
                  <option key="none" value="">
                    No office roles available
                  </option>,
                ]}
          </select>
          <Description>
            Sets the example office; your key list stays the same.
          </Description>
        </Field>
      </div>
      {office && (
        <Text className="mt-3">
          Your roles in {office}: {(profile.roles[office] ?? []).join(", ") || "None"}.
        </Text>
      )}
    </Card>
  );
}
OfficeContext.propTypes = {
  profile: PropTypes.object,
  office: PropTypes.string.isRequired,
  offices: PropTypes.arrayOf(PropTypes.string).isRequired,
  setOfficeChoice: PropTypes.func.isRequired,
};
