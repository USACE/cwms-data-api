import {
  Card,
  Description,
  Dropdown,
  Field,
  Label,
  Skeleton,
  Strong,
  Text,
} from "@usace/groundwork";
import PropTypes from "prop-types";
import { HelpTip } from "../../../components/HelpTip";

export function OfficeSelector({ offices, office, canWrite, loading, onChange }) {
  return (
    <Card className="mb-6 p-5">
      <div className="grid gap-5 md:grid-cols-[minmax(0,20rem)_1fr] md:items-end">
        <Field>
          <div className="flex items-center gap-1">
            <Label>Office</Label>
            <HelpTip title="Office-scoped user lists">
              Each office owns a separate collection of lists. The same list ID may
              exist in two offices without sharing members. Your office role determines
              whether you can view or edit a list.
            </HelpTip>
          </div>
          <Description>
            User lists are isolated by their owning CWMS office.
          </Description>
          {offices.length > 0 ? (
            <Dropdown
              aria-label="Office"
              label="Office"
              labelClassName="sr-only"
              className="mt-0"
              value={office}
              onChange={(event) => onChange(event.target.value)}
              options={offices.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            />
          ) : loading ? (
            <Skeleton className="h-10 w-full" />
          ) : (
            <Text role="status">No authorized CWMS offices are available.</Text>
          )}
        </Field>
        <div className="rounded-lg bg-blue-50 px-4 py-3">
          <Text className="text-blue-900">
            <Strong>{office || "No office selected"}</Strong>
            {canWrite
              ? " administrators can create lists and update membership."
              : " lists are available for viewing with your current role."}
          </Text>
        </div>
      </div>
    </Card>
  );
}

OfficeSelector.propTypes = {
  offices: PropTypes.arrayOf(PropTypes.string).isRequired,
  office: PropTypes.string.isRequired,
  canWrite: PropTypes.bool.isRequired,
  loading: PropTypes.bool.isRequired,
  onChange: PropTypes.func.isRequired,
};
