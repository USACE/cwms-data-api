# Onboarding users

New users must sign in to CDA once to register their account. They may initially
have only HQ roles or no office roles.

## Required administrator roles

To assign a user to an office, your account must have **both `CWMS User Admins`
(Admin) and `CWMS PD Users` (PD) in the destination office**. Having one role in
one office and the other in a different office does not meet this requirement.
Ask an existing administrator to grant the missing roles. The User administrator
preset includes both roles.

The **Onboard users** and **Assign office** actions offer only offices where your
profile has both roles. CDA and the CWMS database enforce authorization on each
request.

## Assign an office and roles

1. Open **User Roles** and click **Onboard users**.
2. Select a user. The list includes users with no active office roles and users
   whose only office is HQ with `All Users`, `CWMS Users`, or both. HQ users with
   additional roles and users assigned to another office are excluded.
3. Choose the destination office and a role preset, or choose **Custom** to select
   specific roles. `All Users` is required and is always included.
4. Click **Assign office and roles**. The modal remains open so you can onboard
   another user. A user assigned outside HQ disappears from the onboarding list
   and appears in the destination office's regular user list.

Existing roles, including HQ membership, are preserved. Onboarding adds roles; use
the regular office role editor to remove roles. Users who remain eligible (for
example, assigned only basic HQ access) remain in the onboarding list. Use
**Refresh users** to load changes made by another administrator.

If a save fails, some grants may already have succeeded. Refresh users and review
their current office roles before retrying. Users now assigned to an office can
be edited in the regular office list. The modal never reports a failed request as
a successful assignment.

For someone already assigned to another office, use **Assign office**, then edit
their roles in the regular office list.
