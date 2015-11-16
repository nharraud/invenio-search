# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Helpers used by tests."""

import os.path

import shutil

import tempfile

# TODO: this class does not have the parameters of the original
# tempfile.TemporaryDirectory
class TemporaryDirectory(object):
    """
    Context manager which creates a temporary directory using
    tempfile.mkdtemp() and deletes it when exiting.
    This class is available in python +v3.2 as tempfile.TemporaryDirectory.
    """

    def __init__(self, nb_dir=1):
        self.nb_dir = nb_dir

    def __enter__(self):
        self.dirs = []
        for i in range(self.nb_dir):
            self.dirs.append(tempfile.mkdtemp())
        return tuple(self.dirs)

    def __exit__(self, exc_type, exc_value, traceback):
        for dir_name in self.dirs:
            shutil.rmtree(dir_name)


# use either the existing class from tempfile or, if it does not exist, the one
# we just created.
TemporaryDirectory = getattr(tempfile, 'TemporaryDirectory',
                             TemporaryDirectory)


def create_file(folder_path, file_name, content):
    """Create a file in the given directory having the given file name
    with the given content."""
    path = os.path.join(folder_path, file_name)
    with open(path, 'w') as file_desc:
        file_desc.write(content)
    return path
