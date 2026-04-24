from __future__ import annotations

project = "mvx-lib"
author = "Vladimir Makarov"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 4,
}

autodoc_typehints = "description"
autodoc_member_order = "bysource"
