import setuptools

"""The setup script."""

if __name__ == '__main__':
    with open("requirements.txt", "r") as filein:
        requirements = filein.readlines()

    with open("requirements-dev.txt", "r") as filein:
        test_requirements = filein.readlines()

    with open("version.txt", "r") as filein:
        version = filein.read()

    with open("README.md", "r") as filein:
        readme = filein.read()

    setup_requirements: list = [
        "setuptools >= 41.0.0",
        # python3 specifically requires wheel 0.26
        'wheel; python_version < "3"',
        'wheel >= 0.26; python_version >= "3"',
    ]

    setuptools.setup(
        name="etl-generic-pipeline-pyspark",
        author="Team 1 Developers",
        author_email="team1developer@example.com",
        description="etl generic pipeline package",
        long_description=readme,
        long_description_content_type="text/markdown",
        url="https://github.com/nagarajuerigi/pyspark_pipleine",
        include_package_data=True,
        package_data={
            "": [
                "version.txt",
                "requirements.txt",
                "requirements-dev.txt",
                "test.py",
                "README.md",
            ]
        },
        project_urls={
            "Bug Tracker": "https://github.com/nagarajuerigi/pyspark_pipleine/issues",
        },
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        package_dir={"": "src"},
        packages=setuptools.find_packages(where="src"),
        python_requires=">=3.6",
        # setup_requires=setup_requirements,
        install_requires=requirements,
        test_suite="etlpackage",
        tests_require=test_requirements,
        version=version,
        zip_safe=False,
    )
