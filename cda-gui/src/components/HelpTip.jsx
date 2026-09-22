import { useId, useState } from "react";
import PropTypes from "prop-types";
import { FaCircleQuestion, FaXmark } from "react-icons/fa6";

export function HelpTip({ title, children, className = "" }) {
  const [open, setOpen] = useState(false);
  const titleId = useId();

  return (
    <div className={`inline-block ${className}`}>
      <button
        type="button"
        aria-expanded={open}
        aria-haspopup="dialog"
        aria-label={`Help: ${title}`}
        title={title}
        className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-full text-blue-700 hover:bg-blue-50 hover:text-blue-900 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-600"
        onClick={() => setOpen(true)}
      >
        <FaCircleQuestion aria-hidden="true" className="h-4 w-4" />
      </button>
      {open && (
        <div
          className="fixed inset-0 z-[100] flex items-center justify-center p-4"
          role="presentation"
          onKeyDown={(event) => {
            if (event.key === "Escape") setOpen(false);
          }}
        >
          <button
            type="button"
            className="absolute inset-0 cursor-default bg-zinc-950/35"
            aria-label="Close help"
            onClick={() => setOpen(false)}
          />
          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby={titleId}
            className="relative z-10 block max-h-[calc(100vh-2rem)] w-full max-w-md overflow-y-auto rounded-lg border border-zinc-200 bg-white p-5 text-left text-sm font-normal leading-5 text-zinc-700 shadow-2xl"
          >
            <div className="mb-3 flex items-start justify-between gap-4">
              <strong id={titleId} className="text-zinc-950">
                {title}
              </strong>
              <button
                type="button"
                autoFocus
                aria-label="Close help"
                className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-zinc-600 hover:bg-zinc-100 hover:text-zinc-950 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-600"
                onClick={() => setOpen(false)}
              >
                <FaXmark aria-hidden="true" />
              </button>
            </div>
            {children}
          </div>
        </div>
      )}
    </div>
  );
}

HelpTip.propTypes = {
  title: PropTypes.string.isRequired,
  children: PropTypes.node.isRequired,
  className: PropTypes.string,
};
