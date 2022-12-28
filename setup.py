from setuptools import setup

setup(
    name='yapim',
    version='0.3.0',
    description='Pipeline generation tool',
    url='',
    author='Christopher Neely',
    author_email='christopher.neely1200@gmail.com',
    license='GNU GPL 3',
    packages=['yapim', 'yapim.utils', 'yapim.tasks', 'yapim.tasks.utils', 'yapim.utils.package_management'],
    python_requires='>=3.8',
    install_requires=[
        "bcbio-gff>=0.6.6",
        "biopython>=1.76",
        "plumbum>=1.6.6",
        "networkx>=2.8.8",
        "pyyaml>=5.3.1",
        "art>=5.1",
        "pylint==2.6.0",
        "pytest>=6.2.1",
        "pytest-cov>=2.10.1"
    ],
    scripts=[
        "yapim/yapim",
    ]
)
