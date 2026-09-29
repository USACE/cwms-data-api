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

Dismissible toasts report creation, copying, refresh, revocation, rotation,
expired keys, and failures. Toasts remain inside the active dialog for keyboard
access and screen-reader announcements. Success messages close after eight seconds
(paused on hover or focus); errors and warnings remain until dismissed or the
user moves to another action. Form errors also remain beside their fields.

The adapter recognizes the validation responses introduced in #1935, including
invalid dates, missing names, malformed JSON, and overlong names. Duplicate names
return actionable conflict feedback. Creation rejects invalid local dates before
sending a request, and replacement names stay within the 64-character limit.
Refreshing clears details for keys that no longer exist.

A 403 from any key operation replaces management controls with a warning page
explaining the required `CWMS Users` permission and signed-in `cac_auth` session.
It directs users to their CWMS Admin and offers an access retry. Recognized missing
roles are highlighted without displaying arbitrary server error details. The
endpoint decides access; office profile roles are not used to infer authorization.

The adapter uses `cwmsjs` raw responses because CDA's bracketed timezone dates
are not parsed by the generated model and DELETE returns an empty 204 body.
The client still handles request serialization and URL encoding.

## Validation

- `npm run test:unit`: request/auth/encoding contracts, creation and expiration, empty
  204 revocation, CDA dates, and safe error messages.
- `npm run lint` and `npm run build`.
- `gradlew :cwms-data-api:test --tests '*SpaErrorStatusFilterTest'` verifies the
  server's direct page route, including the trailing slash.
- `npm run test:api-keys`: Chromium checks for create/copy/refresh/revoke feedback,
  conflict/date-error recovery, rotation failure/cancellation/retry, expired and
  vanished keys, keyboard dismissal, secret storage checks, and 390px layout.
  These tests use synthetic responses and run in the web GUI CI workflow.

## Real local verification

The updated frontend was tested against a local CDA/Oracle/Keycloak stack running
the #1935 endpoint fixes. A uniquely named key created after the page loaded
produced a real 409 conflict and an actionable toast. Removing that fixture allowed
retry in the same dialog, followed by creation, expiration metadata refresh,
closing the one-time secret, and successful empty-body 204 revocation feedback.
The temporary key was removed. Secrets stayed in test-process/component memory
and were excluded from logs and screenshots. Simulated failures were tested
separately in Chromium, including both halves of rotation.
