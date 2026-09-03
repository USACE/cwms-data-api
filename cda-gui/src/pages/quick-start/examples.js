export const publicApi = "https://cwms-data.usace.army.mil/cwms-data";

const timeWindow = {
  begin: "2026-09-01T00:00:00Z",
  end: "2026-09-02T00:00:00Z",
};

export const examples = {
  offices: { path: "/offices", params: { "has-data": "true" } },
  locations: {
    path: "/catalog/LOCATIONS",
    params: { office: "SWT", like: "^KEYS", "page-size": "20" },
  },
  location: { path: "/locations/KEYS", params: { office: "SWT", unit: "EN" } },
  catalog: {
    path: "/catalog/TIMESERIES",
    params: { office: "SWT", like: "^KEYS\\.Elev\\.", "page-size": "20" },
  },
  timeseries: {
    path: "/timeseries",
    params: {
      office: "SWT",
      name: "KEYS.Elev.Inst.1Hour.0.Ccp-Rev",
      ...timeWindow,
      unit: "ft",
    },
  },
  levels: {
    path: "/levels",
    params: {
      office: "SWT",
      "level-id-mask": "KEYS.Elev.Inst.0.*",
      unit: "EN",
      "page-size": "20",
    },
  },
  level: {
    path: "/levels/KEYS.Elev.Inst.0.Top%20of%20Conservation/timeseries",
    params: { office: "SWT", ...timeWindow, interval: "1Day", unit: "ft" },
  },
};

export function exampleUrl(example) {
  return `${publicApi}${example.path}?${new URLSearchParams(example.params)}`;
}
