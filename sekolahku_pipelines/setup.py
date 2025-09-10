from setuptools import find_packages, setup

setup(
    name="sekolahku_pipelines",
    packages=find_packages(exclude=["sekolahku_pipelines_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
