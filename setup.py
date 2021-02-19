import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="windyquery",
    version="0.0.8",
    author="windymile.it",
    author_email="windymile.it@gmail.com",
    description="A non-blocking PostgreSQL query builder using Asyncio",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bluerelay/windyquery",
    packages=setuptools.find_packages(),
    install_requires=[
        'asyncpg==0.21.0',
        'ply==3.11',
    ],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
