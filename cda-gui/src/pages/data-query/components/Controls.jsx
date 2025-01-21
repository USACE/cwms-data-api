import {  Input, Field, Label } from "@usace/groundwork";
import dayjs from "dayjs";

export default function Controls({
  setBeginDateTime,
  setEndDateTime,
  beginDateTime,
  endDateTime,
}) {
  return (
    <div className="flex flex-row justify-around">
      <div className="text-center basis-1/4">
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
      <div className="text-center basis-1/4">
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
