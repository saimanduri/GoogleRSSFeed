"""
Package installation and setup script for RSS Collector.
"""

from setuptools import setup, find_packages
import os

# Read the README file
readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
if os.path.exists(readme_path):
    with open(readme_path, 'r', encoding='utf-8') as f:
        long_description = f.read()
else:
    long_description = 'RSS Collector - An offline-friendly Google News RSS ingestion pipeline'

# Read requirements
requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
if os.path.exists(requirements_path):
    with open(requirements_path, 'r', encoding='utf-8') as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
else:
    requirements = [
        'httpx>=0.24.0',
        'feedparser>=6.0.10',
        'APScheduler>=3.10.0',
        'python-dotenv>=1.0.0',
        'requests>=2.31.0',
    ]

setup(
    name='rss-collector',
    version='1.0.0',
    description='An offline-friendly Google News RSS ingestion pipeline for LLM integration',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='RSS Collector Team',
    author_email='admin@rss-collector.local',
    url='https://github.com/your-org/rss-collector',
    
    # Package discovery
    packages=find_packages(exclude=['tests*']),
    include_package_data=True,
    
    # Dependencies
    install_requires=requirements,
    
    # Optional dependencies
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'black>=23.0.0',
            'flake8>=5.0.0',
            'mypy>=1.0.0',
        ],
        'test': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'responses>=0.23.0',
        ]
    },
    
    # Entry points
    entry_points={
        'console_scripts': [
            'rss-collector=src.main:main',
            'rss-collector-run=run:main',
        ],
    },
    
    # Package data
    package_data={
        'src': ['config/*.json'],
        'tests': ['fixtures/*'],
    },
    
    # Metadata
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content :: News/Diary',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Text Processing :: Markup :: XML',
    ],
    
    # Python version requirement
    python_requires='>=3.10',
    
    # Keywords
    keywords='rss news google feeds offline llm integration',
    
    # Project URLs
    project_urls={
        'Bug Reports': 'https://github.com/your-org/rss-collector/issues',
        'Source': 'https://github.com/your-org/rss-collector',
        'Documentation': 'https://rss-collector.readthedocs.io/',
    },
    
    # License
    license='MIT',
    
    # Zip safe
    zip_safe=False,
)