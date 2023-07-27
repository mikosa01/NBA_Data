from setuptools import setup, find_packages

setup(
    name="NBA Data Pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "numpy",
        "pandas",
        "scikit-learn",
        "pyspark",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "codecov",
            "flake8",
        ]
    },
)
