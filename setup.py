#!/usr/bin/env python
from setuptools import setup, find_packages


def _parse_requirements():
    # make it possible for setup.py to install things from github when reading
    # requirements.txt
    requirements = []

    for line in open("requirements.txt").readlines():
        if line.startswith("#"):
            continue
        elif "git+" in line and "cloudmetrics" in line:
            line = f"cloudmetrics @ {line}"
        elif "git+" in line and "modapsclient" in line:
            line = f"modapsclient @ {line}"
        requirements.append(line)
    return requirements


INSTALL_REQUIRES = _parse_requirements()

setup(
    name="cloudmetrics-pipeline",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="Processing pipeline for the cloud pattern metrics toolkit",
    url="https://github.com/cloudsci/cloudmetrics-pipeline",
    maintainer="Leif Denby",
    maintainer_email="l.c.denby[at]leeds.ac.uk",
    py_modules=["cloudmetrics_pipeline"],
    packages=find_packages(include=["cloudmetrics_pipeline"]),
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    zip_safe=False,
)
