import PropTypes from "prop-types";
import { Badge } from "@usace/groundwork";
import { FaExclamationTriangle } from "react-icons/fa";
import { keyStatus } from "../api";

export default function KeyStatusBadge({ apiKey, now }) {
  const status = keyStatus(apiKey, now);
  return (
    <Badge
      key={status}
      color={
        status === "Expired"
          ? "red"
          : status === "Unknown expiration"
            ? "amber"
            : "green"
      }
    >
      {status === "Expired" && (
        <FaExclamationTriangle aria-hidden="true" className="mr-1" />
      )}
      {status === "Expired" ? "Expired — no longer usable" : status}
    </Badge>
  );
}

KeyStatusBadge.propTypes = {
  apiKey: PropTypes.object.isRequired,
  now: PropTypes.number.isRequired,
};
