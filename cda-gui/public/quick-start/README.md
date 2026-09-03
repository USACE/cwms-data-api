# Getting-started screenshots

Captured September 3, 2026. These are real browser screenshots; the guide draws
numbered highlights with HTML/CSS over the original images.

- `public-home.jpg`: https://cwms-data.usace.army.mil/cwms-data/
- `public-data-query.jpg`: https://cwms-data.usace.army.mil/cwms-data/data-query,
  SWT, manual selection of `KEYS.Elev.Inst.1Hour.0.Ccp-Rev`. The browser shows
  local times for September 2–3; the separate API examples use September 1–2 UTC.
- `local-location-search.jpg`: local `develop` frontend at `/cwms-data/location-search`,
  connected to public CDA, SWT, `like=^KEYS$`. This is explicitly labeled a local
  preview in the guide: public CDA returned a 404 for Location Search at capture
  time and its API did not apply the newer `search-text` parameter.

To refresh, use the same public pages and filters, capture the controls and results,
and update the image dimensions, numbered highlight coordinates, and captions in
`src/pages/quick-start/index.jsx`. Do not replace a public screenshot with a local
preview without updating its provenance label. No authenticated data is shown.

The data-use notice links to the existing USACE water-data disclaimer at
https://www.mvp-wc.usace.army.mil/Disclaimer.html. The footer's external-link
disclaimer describes external links rather than data accuracy.
