import PropTypes from "prop-types";

// Groundwork's current Dropdown keeps its own selection and overrides `value`.
// Office grants must always display the same office that the form will submit.
export function ManagedOfficeSelect({
  id,
  offices,
  value,
  onChange,
  disabled = false,
}) {
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
      {offices.map((office) => (
        <option key={office} value={office}>
          {office}
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
