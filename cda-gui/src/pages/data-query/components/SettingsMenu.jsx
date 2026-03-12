import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import PropTypes from "prop-types";
import Toggle from "./Toggle";
import SettingsGearButton from "./SettingsGearButton";

export default function SettingsMenu({
  cacheEnabled,
  setCacheEnabled,
  sortAscending,
  setSortAscending,
  active,
}) {
  return (
    <Menu as="div" className="relative inline-block text-left">
      <MenuButton as={SettingsGearButton} active={active} />
      <MenuItems className="absolute right-0 z-10 mt-2 w-72 rounded-md border border-slate-200 bg-white p-4 shadow-lg focus:outline-none">
        <MenuItem>
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="text-sm font-semibold text-slate-900">Enable cache</div>
              <p className="text-xs text-slate-600">
                Use browser cache for repeated requests. Disable to force fresh fetches.
              </p>
            </div>
            <Toggle
              checked={cacheEnabled}
              onChange={setCacheEnabled}
              label=""
              className="ml-0"
            />
          </div>
        </MenuItem>
        <MenuItem>
          <div className="mt-4 flex items-start justify-between gap-4">
            <div>
              <div className="text-sm font-semibold text-slate-900">
                Ascending table order
              </div>
              <p className="text-xs text-slate-600">
                Show the oldest timestamps first. Disable to show newest rows first.
              </p>
            </div>
            <Toggle
              checked={sortAscending}
              onChange={setSortAscending}
              label=""
              className="ml-0"
            />
          </div>
        </MenuItem>
      </MenuItems>
    </Menu>
  );
}

SettingsMenu.propTypes = {
  active: PropTypes.bool,
  cacheEnabled: PropTypes.bool.isRequired,
  setCacheEnabled: PropTypes.func.isRequired,
  setSortAscending: PropTypes.func.isRequired,
  sortAscending: PropTypes.bool.isRequired,
};
