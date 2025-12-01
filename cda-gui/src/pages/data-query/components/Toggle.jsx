import { Field, Label, Switch } from "@headlessui/react";
import { gwMerge } from "@usace/groundwork";
import PropTypes from "prop-types";

export default function Toggle({
  checked,
  onChange,
  label = "Toggle setting",
  className = "",
}) {
  return (
    <Field className="flex items-center">
      <Switch
        checked={checked}
        onChange={onChange}
        className={gwMerge(
          "group relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border-2 border-transparent bg-gray-200 transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-indigo-600 focus:ring-offset-2 data-[checked]:bg-indigo-600",
          className,
        )}
      >
        <span
          aria-hidden="true"
          className="pointer-events-none inline-block size-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out group-data-[checked]:translate-x-5"
        />
      </Switch>
      <Label as="span" className="ml-3 text-sm">
        {label}
      </Label>
    </Field>
  );
}

Toggle.propTypes = {
  checked: PropTypes.bool.isRequired,
  onChange: PropTypes.func.isRequired,
  label: PropTypes.string,
  className: PropTypes.string,
};
