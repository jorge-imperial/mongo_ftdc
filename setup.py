# -*- coding: utf-8 -*-

import sys
import re

#Version
VERSIONFILE="version.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

try:
    from skbuild import setup
except ImportError:
    print(
        "Please update pip, you need pip 10 or greater,\n"
        " or you need to install the PEP 518 requirements in pyproject.toml yourself",
        file=sys.stderr,
    )
    raise

from setuptools import find_packages

setup(
    name="pyftdc",
    version=verstr,
    description="A parser for MongoDB FTDC files, with Python bindings.",
    author="Jorge Imperial",
    author_email="jorgeluis.imperial@gmail.com",
    license="Apache v2",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    cmake_install_dir="src/pyftdc",
    include_package_data=True,
    extras_require={"test": ["pytest"]},
    python_requires=">=3.6",
    install_requires=['numpy']

)


