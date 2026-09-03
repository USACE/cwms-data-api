import { useAuth } from "@usace-watermanagement/groundwork-water";
import RequireSignIn from "./components/RequireSignIn";
import KeyManager from "./components/KeyManager";
export default function ApiKeys() {
  const auth = useAuth();
  // Session changes unmount the manager and clear any one-time key secret.
  return (
    <RequireSignIn>
      <KeyManager key={auth.token} token={auth.token} />
    </RequireSignIn>
  );
}
