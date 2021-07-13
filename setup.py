import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="windyquery",
    version="0.0.25",
    author="windymile.it",
    author_email="windymile.it@gmail.com",
    description="A non-blocking PostgreSQL query builder using Asyncio",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bluerelay/windyquery",
    packages=setuptools.find_packages(),
    install_requires=[
        'asyncpg==0.23.0',
        'ply==3.11',
        'python-dateutil==2.8.1',
        'fire==0.4.0',
    ],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        'console_scripts': [
            'wq=windyquery.scripts:main',
        ],
    },
)
