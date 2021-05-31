#!/usr/bin/env python3
import os

import setuptools

version = {}
with open("denorm/version.py", "r") as f:
    exec(f.read(), version)

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    author="Rivet Health",
    author_email="ops@rivethealth.com",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2",
    ],
    description="Maintain denormalized and aggregated PostgreSQL tables",
    entry_points={
        "console_scripts": [
            "denorm=denorm.cli.main:main",
        ]
    },
    extras_require={"dev": ["black", "jsonschema2md", "pytest-env", "isort", "pytest"]},
    package_data={
        "denorm.formats": ["*.json"],
    },
    install_requires=["dataclasses-json", "jsonschema", "pg-sql"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    name="denorm",
    packages=setuptools.find_packages(),
    project_urls={
        "Issues": "https://github.com/rivethealth/denorm/issues",
    },
    url="https://github.com/rivethealth/denorm",
    version=version["__version__"],
)
