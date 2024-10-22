import { Button } from "@usace/groundwork";
import { FiLogIn } from "react-icons/fi";
import { getOrigin, getPathname } from "../utils/base";

const HOSTNAME = getOrigin()
function handleLogin() {
    // This will not work via the reverse proxy
    // And we cannot hard set the port here due to security reasons
    window.location.href = `/CWMSLogin/login?OriginalLocation=${window.location.href}`;
}
export default function LoginButton() {
  // Do not show the Login on the public side
  if (!HOSTNAME.toLowerCase().includes(".ds.") 
        && !HOSTNAME.includes("local")) return;
  return (
    <Button
      missiontext="Login"
      style="plain"
      color="white"
      className="mx-5"
      size="lg"
      onClick={handleLogin}
    >
      CDA Login <FiLogIn />
    </Button>
  );
}
