"""Nox sessions."""
import tempfile
from typing import Any

import nox
from nox.sessions import Session

package = "cdap_geo_utils"
nox.options.sessions = "isort", "lint", "safety", "mypy", "tests"
locations = "src", "tests", "noxfile.py"
# locations = "src", "tests", "noxfile.py", "docs/source/conf.py"


def install_with_constraints(session: Session, *args: str, **kwargs: Any) -> None:
    """Install packages constrained by Poetry's lock file.

    This function is a wrapper for nox.sessions.Session.install. It
    invokes pip to install packages inside of the session's virtualenv.
    Additionally, pip is passed a constraints file generated from
    Poetry's lock file, to ensure that the packages are pinned to the
    versions specified in poetry.lock. This allows you to manage the
    packages as Poetry development dependencies.

    Arguments:
        session: The Session object.
        args: Command-line arguments for pip.
        kwargs: Additional keyword arguments for Session.install.

    """
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


@nox.session(python="3.8")
def isort(session: Session) -> None:
    """Sort imports with isort."""
    args = session.posargs or locations
    install_with_constraints(session, "isort")
    session.run("isort", *args)


@nox.session(python="3.8")
def black(session: Session) -> None:
    """Run black code formatter."""
    args = session.posargs or locations
    install_with_constraints(session, "black")
    session.run("black", *args)


@nox.session(python="3.8")
def lint(session: Session) -> None:
    """Lint using flake8."""
    args = session.posargs or locations
    install_with_constraints(
        session,
        "flake8",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-isort",
        "darglint",
    )
    session.run("flake8", *args)


@nox.session(python="3.8")
def safety(session: Session) -> None:
    """Scan dependencies for insecure packages."""
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        install_with_constraints(session, "safety")
        session.run(
            "safety",
            "check",
            f"--file={requirements.name}",
            "--full-report",
            # ! Ignoring recommendation to upgrade to GDAL 3.3.3 because the
            # ! vulnerability affects netcdf, which we aren't using, and 3.3.2
            # ! is the latest version available through ubuntugis-unstable as of
            # ! 2021-12-07.
            "--ignore=42369",
        )


@nox.session(python="3.8")
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or locations
    install_with_constraints(session, "mypy")
    session.run("mypy", *args, "--ignore-missing-imports")


@nox.session(python="3.8")
def tests(session: Session) -> None:
    """Run the test suite."""
    args = session.posargs or [
        "--cov",
        "-vv",
    ]
    session.run("poetry", "install", "--no-dev", external=True)
    install_with_constraints(
        session,
        "coverage[toml]",
        "pytest",
        "pytest-cov",
        "geopandas",
        "chispa",
        "pyarrow",
    )
    session.run("pytest", *args)


# @nox.session(python="3.8")
# def coverage(session: Session) -> None:
#     """Upload coverage data."""
#     install_with_constraints(session, "coverage[toml]", "codecov")
#     session.run("coverage", "xml", "--fail-under=0")
#     session.run("codecov", *session.posargs)


# @nox.session(python="3.8")
# def docs(session: Session) -> None:
#     """Build the documentation."""
#     session.run("poetry", "install", "--no-dev", external=True)
#     install_with_constraints(session, "sphinx")
#     session.run("sphinx-build", "docs/source", "docs/_build")
