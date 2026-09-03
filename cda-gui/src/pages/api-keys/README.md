# API Keys

Signed-in users can open `/cwms-data/api-keys` from the API Keys navigation link
or from any of the four Authorization key operations in Swagger. Signed-out
visitors are redirected to Home, and the navigation link is hidden.

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
  context, help, create/one-time secret, storage checks, revoke cancellation and
  success, 403 recovery, signed-out direct route/navigation, and 390px layout.
  Screenshots contain mock users and key names, with no secrets.

Live database key operations and a deployed WAR still need integration testing.
