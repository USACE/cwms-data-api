import { useAuth } from "@usace-watermanagement/groundwork-water";
import { LoginButton } from "@usace/groundwork";

export default function AuthButton() {
  const auth = useAuth();

  return auth.isAuth ? (
    <button className="text-white underline" type="button" onClick={auth.logout}>
      Log out
    </button>
  ) : (
    <LoginButton onClick={auth.login} />
  );
}
