import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import PropTypes from "prop-types";
import Toggle from "./Toggle";
import SettingsGearButton from "./SettingsGearButton";

export default function SettingsMenu({
  cacheEnabled,
  setCacheEnabled,
  sortAscending,
  setSortAscending,
  includeMissingTimeseries,
  setIncludeMissingTimeseries,
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
              className="ml-0"
            />
          </div>
        </MenuItem>
        <MenuItem>
          <div className="mt-4 flex items-start justify-between gap-4">
            <div>
              <div className="text-sm font-semibold text-slate-900">
                Descending table order
              </div>
              <p className="text-xs text-slate-600">
                Keep newest timestamps first. Disable to switch to oldest rows first.
              </p>
            </div>
            <Toggle
              checked={!sortAscending}
              onChange={(checked) => setSortAscending(!checked)}
              className="ml-0"
            />
          </div>
        </MenuItem>
        <MenuItem>
          <div className="mt-4 flex items-start justify-between gap-4">
            <div>
              <div className="text-sm font-semibold text-slate-900">
                Include missing time series
              </div>
              <p className="text-xs text-slate-600">
                Show catalog matches that have no extents in smart select.
              </p>
            </div>
            <Toggle
              checked={includeMissingTimeseries}
              onChange={setIncludeMissingTimeseries}
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
  includeMissingTimeseries: PropTypes.bool.isRequired,
  setCacheEnabled: PropTypes.func.isRequired,
  setIncludeMissingTimeseries: PropTypes.func.isRequired,
  setSortAscending: PropTypes.func.isRequired,
  sortAscending: PropTypes.bool.isRequired,
};
