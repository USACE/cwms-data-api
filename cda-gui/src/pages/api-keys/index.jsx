import { useAuth } from "@usace-watermanagement/groundwork-water";
import KeyManager from "./components/KeyManager";
export default function ApiKeys() {
  const auth = useAuth();
  return <KeyManager token={auth.token} />;
}
