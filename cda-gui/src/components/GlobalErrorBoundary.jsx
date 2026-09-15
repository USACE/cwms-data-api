import PropTypes from "prop-types";
import { Component } from "react";

export default class GlobalErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, errorInfo) {
    console.error("Uncaught CDA GUI error", error, errorInfo);
  }

  render() {
    if (this.state.error) {
      return (
        <main className="p-6">
          <h1 className="text-lg font-semibold text-red-700">Something went wrong</h1>
          <p className="mt-2 text-slate-700">
            {this.state.error?.message ?? "An unexpected error occurred."}
          </p>
          <a className="mt-4 inline-block text-blue-700 underline" href="/">
            Return to CDA
          </a>
        </main>
      );
    }

    return this.props.children;
  }
}

GlobalErrorBoundary.propTypes = {
  children: PropTypes.node.isRequired,
};
