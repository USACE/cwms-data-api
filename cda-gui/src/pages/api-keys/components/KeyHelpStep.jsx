import PropTypes from "prop-types";
import { Card, H2 } from "@usace/groundwork";

export default function KeyHelpStep({ number, title, children }) {
  return (
    <li>
      <Card className="min-w-0 p-5 sm:p-7">
        <div className="mb-4 flex items-center gap-3">
          <span
            aria-hidden="true"
            className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-blue-100 font-bold text-blue-800"
          >
            {number}
          </span>
          <H2 className="text-xl">
            <span className="sr-only">Step {number}: </span>
            {title}
          </H2>
        </div>
        <div className="space-y-3">{children}</div>
      </Card>
    </li>
  );
}

KeyHelpStep.propTypes = {
  number: PropTypes.number.isRequired,
  title: PropTypes.string.isRequired,
  children: PropTypes.node.isRequired,
};
