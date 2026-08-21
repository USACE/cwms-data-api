import { forwardRef } from "react";
import PropTypes from "prop-types";
import { FiSettings } from "react-icons/fi";

const SettingsGearButton = forwardRef(function SettingsGearButton(
  { active = false, className = "", ...props },
  ref,
) {
  const activeClassName = active
    ? "border-red-200 bg-red-50 text-red-600 hover:bg-red-100"
    : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50";

  return (
    <button
      {...props}
      ref={ref}
      type="button"
      className={`inline-flex items-center justify-center gap-2 rounded border px-3 py-2 shadow-sm transition ${activeClassName} ${className}`.trim()}
    >
      <span className="text-sm font-medium">Query Settings</span>
      <FiSettings className="h-5 w-5" aria-hidden="true" />
      <span className="sr-only">Open query settings</span>
    </button>
  );
});

SettingsGearButton.propTypes = {
  active: PropTypes.bool,
  className: PropTypes.string,
};

export default SettingsGearButton;
