import PropTypes from "prop-types";
import dayjs from "dayjs";
import { Button } from "@usace/groundwork";

export default function ExpirationShortcuts({ setExpires, working }) {
  function setExpiration(amount, unit) {
    const today = dayjs(new Date().toISOString().slice(0, 10));
    setExpires(today.add(amount, unit).format("YYYY-MM-DD"));
  }

  return (
    <div
      className="mt-3 flex flex-wrap gap-2"
      role="group"
      aria-label="Expiration shortcuts"
    >
      {[
        ["30d", 30, "day"],
        ["90d", 90, "day"],
        ["1y", 1, "year"],
      ].map(([label, amount, unit]) => (
        <Button
          key={label}
          type="button"
          color="light"
          disabled={working}
          aria-label={`Expire in ${amount} ${unit}${amount === 1 ? "" : "s"}`}
          onClick={() => setExpiration(amount, unit)}
        >
          {label}
        </Button>
      ))}
      <Button
        type="button"
        color="light"
        disabled={working}
        onClick={() => setExpires("")}
      >
        Clear date
      </Button>
    </div>
  );
}

ExpirationShortcuts.propTypes = {
  setExpires: PropTypes.func.isRequired,
  working: PropTypes.bool.isRequired,
};
