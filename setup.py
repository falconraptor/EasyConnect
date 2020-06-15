import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="EasyConnect",
    version="1.0.5",
    author="Dustin Surwill",
    author_email="dustinsurwill@gmail.com",
    description="A custom connection pool for pyodbc, pypyodbc, pymysql. Includes custom version of pypyodbc which only supports MS SQL Servers and Python 3+.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/falconraptor/EasyConnect",
    packages=setuptools.find_packages(exclude=['__pycache__', 'tests']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database"
    ],
    python_requires='>=3.6',
    package_data={'litespeed': ['html/*.html']},
    include_package_data=True
)

