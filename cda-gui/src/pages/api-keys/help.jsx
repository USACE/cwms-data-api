import RequireSignIn from "./components/RequireSignIn";
import KeyHelpGuide from "./components/KeyHelpGuide";

export default function ApiKeyHelp() {
  return (
    <RequireSignIn>
      <KeyHelpGuide />
    </RequireSignIn>
  );
}
