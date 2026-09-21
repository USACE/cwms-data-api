import { Fragment, useState } from "react";
import PropTypes from "prop-types";
import { Button, Card, H1, Skeleton, Text } from "@usace/groundwork";
import { useSessionAuth } from "./use-session-auth";
import { useAuthConfiguration } from "./auth-configuration-context";

export default function RequireSignIn({ children }) {
  const auth = useSessionAuth();
  const { error } = useAuthConfiguration();
  const [loginError, setLoginError] = useState("");

  if (auth.isAuth && auth.profile) {
    // Rotate credentials without remounting. A different user gets fresh state.
    return <Fragment key={auth.profile.userName}>{children}</Fragment>;
  }
  if (auth.isLoading) return <Skeleton className="my-8 h-40 w-full" />;

  async function login() {
    setLoginError("");
    try {
      await auth.login({ redirectUri: window.location.href });
    } catch {
      setLoginError("Unable to log in. Please try again.");
    }
  }

  return (
    <Card className="mx-auto my-12 max-w-2xl p-8 text-center">
      <H1>{auth.isAuth ? "Profile unavailable" : "Login required"}</H1>
      <Text className="mt-3">
        {auth.isAuth
          ? "Your user profile could not be loaded. Please try again."
          : "You must log in to view this page."}
      </Text>
      {error || loginError ? <p role="alert">{error || loginError}</p> : null}
      {!error && !auth.isAuth && (
        <Button className="mt-6" type="button" onClick={login}>
          Log in
        </Button>
      )}
    </Card>
  );
}

RequireSignIn.propTypes = { children: PropTypes.node.isRequired };
