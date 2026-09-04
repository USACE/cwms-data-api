# API Keys

Signed-in users can open `/cwms-data/api-keys` from the API Keys navigation link
or from any of the four Authorization key operations in Swagger. Signed-out
visitors are redirected to Home, and the navigation link is hidden.

`/cwms-data/api-keys/help` is a separate signed-in guide with four numbered
steps: create, save, send a request, and replace or revoke. It preserves office
context when returning to key management and has a separate troubleshooting
section. Both routes are excluded from the public sitemap.

Page entrypoints compose the files in `components/`: authentication guard,
manager, header, office context, key list/details, individual dialogs, and guide
sections. API requests and date handling remain in `api.js`.

The page uses Groundwork controls and dialogs, Groundwork Water's authentication
and profile provider, and the existing `cwmsjs` Authorization API. No unpublished
shared-library changes are required. Its card layout follows the User Roles page
in CDA PR #1903 while remaining independently mergeable.

Keys belong to the current user. The existing key endpoints have no office
parameter: the office selector shows the user's roles and sets the office in the
help example, without filtering keys or limiting their authority. Creation uses
the CWMS profile's user name rather than accepting another user's ID.

The secret is retained only in component memory until the user acknowledges
saving it. It is excluded from list/detail state and never put in browser
storage or query caches. Session changes and navigation unmount that state and
abort pending requests. List/detail calls return metadata only. Revocation has
an explicit confirmation step.

Rotation creates a uniquely named replacement first, displays its secret, then
offers to revoke the old key after the user has saved it and updated their
application. Cancelling the final confirmation keeps both keys. Creation failure
does not revoke the old key; revocation failure leaves the replacement available
and allows retry. Existing endpoints do not offer atomic same-name rotation.

Expired keys have red list styling, a warning icon/badge, and a detail notice
explaining they cannot authenticate. Status refreshes while the page is open.
Request failures, including generic server errors, display plain-language messages
without raw response bodies or status codes. Uncertain server failures direct the
user to refresh before retrying a change.

The adapter uses `cwmsjs` raw responses because CDA's bracketed timezone dates
are not parsed by the generated model and DELETE returns an empty 204 body.
The client still handles request serialization and URL encoding.

## Validation

- `npm test`: request/auth/encoding contracts, creation and expiration, empty
  204 revocation, CDA dates, and safe error messages.
- `npm run lint` and `npm run build`.
- `gradlew :cwms-data-api:test --tests '*SpaErrorStatusFilterTest'` verifies the
  server's direct page route, including the trailing slash.
- Browser checks with mocked CDA responses: signed-in list/detail, office
  context, guide navigation/direct reload and return-office selection,
  create/one-time secret, storage checks, revoke cancellation and
  success, 403 recovery, signed-out direct routes/navigation, and 390px layout
  including wrapped code examples.
  Screenshots contain mock users and key names, with no secrets.

## Real local verification

The current frontend was tested against the existing local CDA/Oracle/Keycloak
stack using its `m5hectest` fixture account. Four uniquely named test keys were
created and revoked: a normal key, its rotated replacement, a key without an
expiration, and an expired fixture created through the API.

The test verified authentication with new keys, rejection after revocation,
continued original-key access until rotation confirmation, replacement-key access
after rotation, expired-key rejection and UI styling, and an empty test-key list
after cleanup. Secrets were kept in test-process memory and excluded from logs
and screenshots.

The existing CDA runtime identifies itself as `2026.08.05-1733-user-lists-ui`;
this verifies the current frontend against real existing key endpoints, not a
newly deployed WAR. A localhost-only test proxy isolated this stack from a second
local CDA stack with conflicting proxy routes. Simulated server failures were
tested separately in the browser, including both halves of rotation.
