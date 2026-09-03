import { useAuth } from "@usace-watermanagement/groundwork-water";
import { LoginButton } from "@usace/groundwork";
import { useAuthConfiguration } from "./auth-configuration-context";

export default function AuthButton() {
  const auth = useAuth();
  const { error } = useAuthConfiguration();

  if (error) {
    return (
      <span className="text-sm text-white" title={error}>
        Sign-in unavailable
      </span>
    );
  }

  return auth.isAuth ? (
    <button className="text-white underline" type="button" onClick={auth.logout}>
      Log out
    </button>
  ) : (
    <LoginButton onClick={auth.login} />
  );
}
