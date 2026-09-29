import { useEffect, useState } from "react";
import PropTypes from "prop-types";
import {
  FaCheckCircle,
  FaExclamationTriangle,
  FaInfoCircle,
  FaTimes,
} from "react-icons/fa";

const icons = {
  success: FaCheckCircle,
  error: FaExclamationTriangle,
  warning: FaExclamationTriangle,
  info: FaInfoCircle,
};

// Render within the active dialog so its focus trap and inert background do not
// hide the notification or prevent keyboard users from dismissing it.
export default function KeyFeedback({ notification, onDismiss }) {
  const [paused, setPaused] = useState(false);
  useEffect(() => {
    if (!notification || paused || ["error", "warning"].includes(notification.kind))
      return;
    const timer = window.setTimeout(onDismiss, 8000);
    return () => window.clearTimeout(timer);
  }, [notification, onDismiss, paused]);
  if (!notification) return null;
  const Icon = icons[notification.kind];
  return (
    <div
      className={`api-key-toast api-key-toast-${notification.kind}`}
      onMouseEnter={() => setPaused(true)}
      onMouseLeave={() => setPaused(false)}
      onFocus={() => setPaused(true)}
      onBlur={() => setPaused(false)}
    >
      <div
        key={notification.id}
        role={notification.kind === "error" ? "alert" : "status"}
        aria-atomic="true"
        className="flex min-w-0 gap-3"
      >
        <Icon aria-hidden="true" className="mt-1 shrink-0" />
        <p className="min-w-0 break-words">{notification.message}</p>
      </div>
      <button
        type="button"
        aria-label="Dismiss notification"
        className="rounded p-2 focus-visible:outline focus-visible:outline-2"
        onClick={onDismiss}
      >
        <FaTimes aria-hidden="true" />
      </button>
    </div>
  );
}

KeyFeedback.propTypes = {
  notification: PropTypes.shape({
    id: PropTypes.number.isRequired,
    kind: PropTypes.oneOf(["success", "error", "warning", "info"]).isRequired,
    message: PropTypes.string.isRequired,
  }),
  onDismiss: PropTypes.func.isRequired,
};
