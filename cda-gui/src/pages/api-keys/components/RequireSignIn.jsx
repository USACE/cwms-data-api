import PropTypes from "prop-types";
import { Navigate } from "react-router-dom";
import { useAuth } from "@usace-watermanagement/groundwork-water";
import { Skeleton } from "@usace/groundwork";

export default function RequireSignIn({ children }) {
  const auth = useAuth();
  if (auth.isLoading) return <Skeleton className="my-8 h-40 w-full" />;
  if (!auth.isAuth) return <Navigate to="/" replace />;
  return children;
}

RequireSignIn.propTypes = { children: PropTypes.node.isRequired };
