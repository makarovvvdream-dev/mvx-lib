from pathlib import Path
import tomllib

project = "mvx-lib"
author = "Vladimir Makarov"

_REPO_ROOT = Path(__file__).resolve().parents[1]

_MVX_COMMON_PYPROJECT = _REPO_ROOT / "common" / "pyproject.toml"
_MVX_COMMON_PROJECT = tomllib.loads(_MVX_COMMON_PYPROJECT.read_text(encoding="utf-8"))[
    "project"
]

myst_enable_extensions = [
    "substitution",
]

myst_substitutions = {
    "mvx_common_package": _MVX_COMMON_PROJECT["name"],
    "mvx_common_version": _MVX_COMMON_PROJECT["version"],
}

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "myst_parser",
    "enum_tools.autoenum",
]

html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 5,
}
html_css_files = ["custom.css"]

autodoc_typehints = "description"
autodoc_member_order = "bysource"

html_static_path = ["_static"]
html_logo = "_static/MariVeX.png"

html_context = {
    "display_github": True,
    "github_user": "makarovvvdream-dev",
    "github_repo": "mvx-lib",
    "github_version": "main",
    "conf_py_path": "/docs/",
}
