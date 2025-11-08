# Configuration file for the Sphinx documentation builder.

# -- Project information

project = "CWMS Data API"
copyright = "Public Domain"
author = "HEC, Various"

# TODO: Sort this out later, what version? Does this get used?
release = "0.0"
version = "0.0.0"

# -- General configuration

# TODO: sphinxjs? direct use of API in the docs?
extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "myst_parser", # enables Markdown via MyST
    "sphinxcontrib.mermaid", # render Mermaid
]

# Recognize both .rst and .md files
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# MyST: treat ```mermaid blocks as directives so they render (not just highlight)
myst_fence_as_directive = ["mermaid"]

# Optional MyST settings (safe defaults)
myst_enable_extensions = [
    "deflist",
    "substitution",
    "tasklist",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]
html_static_path = ['_static']
html_css_files = ['custom.css']

# -- Options for HTML output

html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "navigation_depth": 4,
    "collapse_navigation": False,
    "includehidden": True,
}


# -- Options for EPUB output
epub_show_urls = "footnote"
