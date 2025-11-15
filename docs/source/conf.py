# Configuration file for the Sphinx documentation builder.
import os, json, urllib.request

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
    # OpenAPI / API reference extensions
    "sphinxcontrib.openapi",
    "sphinxcontrib.redoc",
    "sphinx_design",
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
    "cwmsdb": ("https://cwms-database.readthedocs.io/en/latest/", None),

}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]
html_static_path = ['_static']
html_css_files = ['custom.css']

# -- Options for HTML output

html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "navigation_depth": 3,
    "collapse_navigation": False,
    "includehidden": False, #avoid pulling anchors/hidden items into the sidebar
}


# -- Options for EPUB output
epub_show_urls = "footnote"



here = os.path.dirname(__file__)
out_dir = os.path.join(here, '..', 'build', 'openapi')
os.makedirs(out_dir, exist_ok=True)
openapi_json = os.path.join(out_dir, 'openapi.json')

OPENAPI_URL = os.environ.get('OPENAPI_URL', 'https://cwms-data.usace.army.mil/cwms-data/swagger-docs')  # default to public instance
if not os.path.exists(openapi_json):
    try:
        with urllib.request.urlopen(OPENAPI_URL) as resp:
            data = resp.read()
            with open(openapi_json, 'wb') as f:
                f.write(data)
    except Exception:
        # Fall back to a vendored copy if live fetch isn’t available
        vendored = os.path.join(here, 'openapi', 'openapi.json')
        if os.path.exists(vendored):
            with open(vendored, 'rb') as src, open(openapi_json, 'wb') as dst:
                dst.write(src.read())
