import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="PyEasySQL",
    version="1.2.1",
    license='MIT License',
    author="Ashenguard",
    author_email="Ashenguard@agmdev.com",
    description="SQL Database management without even a SQL line",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ashengaurd/easysql",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    include_package_data=True,
    install_requires=["mysql-connector"],
)