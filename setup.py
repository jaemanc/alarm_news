"""
Setup configuration for Alarm News System
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="alarm-news",
    version="0.1.0",
    author="Alarm News Team",
    description="A distributed, event-driven email notification service for personalized news and stock alerts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/alarm_news",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Communications :: Email",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "alarm-news-auth=auth.main:main",
            "alarm-news-crawler=crawler.main:main",
            "alarm-news-scheduler=scheduler.main:main",
            "alarm-news-worker=worker.main:main",
            "alarm-news-email-worker=email_worker.main:main",
        ],
    },
)
