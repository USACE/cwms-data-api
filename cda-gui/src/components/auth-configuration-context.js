import { createContext, useContext } from "react";

export const AuthConfigurationContext = createContext({ error: null });

export function useAuthConfiguration() {
  return useContext(AuthConfigurationContext);
}
