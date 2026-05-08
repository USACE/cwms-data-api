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
    <div className="flex flex-col sm:flex-row gap-4 sm:justify-around">
      <div className="text-center w-full sm:basis-1/3">
        <div className="dropdown-select">
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
            />
          </Field>
        </div>
      </div>
      <div className="text-center w-full sm:basis-1/3">
        <div className="dropdown-select">
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
