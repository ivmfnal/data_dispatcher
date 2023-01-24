import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname), "r").read()

def get_version():
    g = {}
    exec(open(os.path.join("data_dispatcher", "version.py"), "r").read(), g)
    return g["Version"]


setup(
    name = "datadispatcher",
    version = get_version(),
    author = "Igor Mandrichenko",
    author_email = "ivm@fnal.gov",
    description = ("Data Dispatcher - core workflow management"),
    license = "BSD 3-clause",
    keywords = "workflow management, data management, web service",
    url = "https://github.com/ivmfnal/data_dispatcher",
    packages=['data_dispatcher', 'data_dispatcher.logs', 'data_dispatcher.ui', 'data_dispatcher.query', 
                    'data_dispatcher.ui.cli', 'data_dispatcher.ddsam'],
    install_requires=["metacat>=3.26.0", "requests"],
    zip_safe = False,
    classifiers=[
    ],
    entry_points = {
        "console_scripts": [
            "ddisp = data_dispatcher.ui.ui_main:main",
            "dd = data_dispatcher.ui.ui_main:main",
            "dd-sam = data_dispatcher.ddsam.ddsam:main"
        ]
    }
)
