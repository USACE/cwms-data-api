import { Input, Field, Label } from "@usace/groundwork";
import PropTypes from "prop-types";
import dayjs from "dayjs";

export default function Controls({
  setBeginDateTime,
  setEndDateTime,
  beginDateTime,
  endDateTime,
}) {
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
      <div className="w-full min-w-0">
        <div className="dropdown-select w-full">
          <Field>
            <Label>Begin Datetime</Label>
            <Input
              onChange={(e) => {
                setBeginDateTime(dayjs(e.target.value));
              }}
              value={beginDateTime.format("YYYY-MM-DDTHH:mm")}
              invalid={false}
              type="datetime-local"
              placeholder="datetime-local"
              label="label"
              className="w-full min-w-0"
            />
          </Field>
        </div>
      </div>
      <div className="w-full min-w-0">
        <div className="dropdown-select w-full">
          <Field>
            <Label>End Datetime</Label>
            <Input
              onChange={(e) => {
                setEndDateTime(dayjs(e.target.value));
              }}
              value={endDateTime.format("YYYY-MM-DDTHH:mm")}
              invalid={false}
              type="datetime-local"
              placeholder="datetime-local"
              label="label"
              className="w-full min-w-0"
            />
          </Field>
        </div>
      </div>
    </div>
  );
}

Controls.propTypes = {
  setBeginDateTime: PropTypes.func.isRequired,
  setEndDateTime: PropTypes.func.isRequired,
  beginDateTime: PropTypes.object.isRequired,
  endDateTime: PropTypes.object.isRequired,
};
