import PropTypes from "prop-types";
import { useCdaOffices } from "@usace-watermanagement/groundwork-water";

// Groundwork's current Dropdown keeps its own selection and overrides `value`.
// Office grants must always display the same office that the form will submit.
export function ManagedOfficeSelect({
  id,
  offices,
  value,
  onChange,
  disabled = false,
}) {
  const { data = [] } = useCdaOffices({
    cdaUrl: import.meta.env.VITE_CDA_API_ROOT,
  });
  const names = new Map(data.map((office) => [office.name, office.longName]));
  if (!names.get("HQ")) names.set("HQ", "Headquarters");
  return (
    <select
      id={id}
      aria-label="Select an office"
      className="w-full rounded border border-zinc-300 bg-white p-2"
      value={value}
      disabled={disabled}
      onChange={(event) => onChange(event.target.value)}
    >
      {!value && <option value="">Select an office</option>}
      {[...offices]
        .sort((left, right) => left.localeCompare(right))
        .map((office) => (
          <option key={office} value={office}>
            {names.get(office) ? `${office} - ${names.get(office)}` : office}
          </option>
        ))}
    </select>
  );
}

ManagedOfficeSelect.propTypes = {
  id: PropTypes.string.isRequired,
  offices: PropTypes.arrayOf(PropTypes.string).isRequired,
  value: PropTypes.string.isRequired,
  onChange: PropTypes.func.isRequired,
  disabled: PropTypes.bool,
};
