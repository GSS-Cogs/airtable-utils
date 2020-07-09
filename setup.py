import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="reposync", # Replace with your own username
    version="0.0.1",
    author="Leigh Perryman",
    author_email="Leigh.Perryman@ons.gov.uk",
    description="Sync a local git repository with Airtables, Git and Jenkins",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gsscogs/airtable-utils",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    setup_requires=['setuptools-git-version'],
    install_requires=['airtable-python-wrapper', 'pygithub', 'python-jenkins', 'lxml>=4.4.0', 'progress'],
    entry_points={
        'console_scripts': ['repo-sync=reposync.reposync:sync']
    },
    include_package_data=True,
)
