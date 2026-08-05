import { H3, Strong, Text } from "@usace/groundwork";
import PropTypes from "prop-types";

export function Notice({ kind, children }) {
  const isError = kind === "error";
  return (
    <div
      role={isError ? "alert" : "status"}
      className={`mb-5 rounded-lg border px-4 py-3 ${
        isError
          ? "border-red-200 bg-red-50 text-red-800"
          : "border-emerald-200 bg-emerald-50 text-emerald-800"
      }`}
    >
      {isError ? (
        <Text className="text-red-800">{children}</Text>
      ) : (
        <Strong>{children}</Strong>
      )}
    </div>
  );
}

export function EmptyState({ icon: Icon, title, children }) {
  return (
    <div className="flex min-h-52 flex-col items-center justify-center rounded-lg border border-dashed border-zinc-300 bg-zinc-50 px-6 py-10 text-center">
      <div className="mb-3 rounded-full bg-white p-3 text-zinc-500 shadow-sm">
        <Icon aria-hidden="true" className="h-6 w-6" />
      </div>
      <H3 className="text-base">{title}</H3>
      <Text className="mt-1 max-w-sm">{children}</Text>
    </div>
  );
}

Notice.propTypes = {
  kind: PropTypes.oneOf(["error", "success"]).isRequired,
  children: PropTypes.node.isRequired,
};

EmptyState.propTypes = {
  icon: PropTypes.elementType.isRequired,
  title: PropTypes.string.isRequired,
  children: PropTypes.node.isRequired,
};
