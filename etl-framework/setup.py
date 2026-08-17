"""Setup configuration for the ETL Framework package."""

from setuptools import setup, find_packages
from pathlib import Path

# Read version from VERSION file
version = Path(__file__).parent.joinpath("VERSION").read_text().strip()

setup(
    name="ETLFramework",
    version=version,
    description="A configuration-driven, extensible ETL framework built on AWS Glue",
    long_description=Path(__file__).parent.joinpath("README.md").read_text(),
    long_description_content_type="text/markdown",
    author="AWS Samples",
    license="MIT-0",
    python_requires=">=3.10",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    package_data={
        "etl_framework": ["**/*.json", "**/*.sql"],
    },
    install_requires=[
        "boto3>=1.26.0",
        "pyspark>=3.3.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "pytest-mock>=3.10",
            "moto[all]>=4.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
