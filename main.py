#!/usr/bin/env python
""" Python script to manage pontential ghost switches between REDCap projects.
Part of the ICARIA Clinical Trial"""

import ghosts

__author__ = "Andreu Bofill Pumarola"
__copyright__ = "Copyright 2024, ISGlobal Maternal, Child and Reproductive Health"
__credits__ = ["Andreu Bofill Pumarola"]
__license__ = "MIT"
__version__ = "0.0.1"
__date__ = "20240206"
__maintainer__ = "Andreu Bofill"
__email__ = "andreu.bofill@isglobal.org"
__status__ = "Dev"


if __name__ == '__main__':
    ghosts.define_ghost_records()