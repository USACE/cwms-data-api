import { useEffect, useState } from "react";
import { useAuth } from "@usace-watermanagement/groundwork-water";

// The profile query changes keys when the access token rotates. Keep the last
// profile during that fetch, but never carry it past logout or a failed fetch.
export function useSessionAuth() {
  const auth = useAuth();
  const [previousProfile, setPreviousProfile] = useState(auth.profile);
  useEffect(() => {
    if (!auth.isAuth || !auth.isLoading || auth.profile) {
      setPreviousProfile(auth.isAuth ? auth.profile : undefined);
    }
  }, [auth.isAuth, auth.isLoading, auth.profile]);
  return {
    ...auth,
    profile: auth.isAuth
      ? (auth.profile ?? (auth.isLoading ? previousProfile : undefined))
      : undefined,
  };
}
