import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read().replace(
        "](", "](https://github.com/Kasekopf/SlurmQueen/blob/master/"
    )

setuptools.setup(
    name="slurmqueen",
    version="1.6.1",
    author="Jeffrey Dudek",
    author_email="jeffreydudek@gmail.com",
    description="A Python 3 library for automatically running experiments using a Slurm cluster",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Kasekopf/SlurmQueen",
    packages=setuptools.find_packages(),
    license="MIT",
    platforms="Posix; MacOS X; Windows",
    install_requires=["paramiko", "pandas"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
