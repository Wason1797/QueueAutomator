import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="queue-automator",
    version="0.0.2",
    author="Wason1797",
    author_email="wabrborich@hotmail.com",
    description="A simple wrapper to build queue multiprocessing pipelines",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Wason1797/QueueAutomator",
    project_urls={
        "Bug Tracker": "https://github.com/Wason1797/QueueAutomator/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
)
