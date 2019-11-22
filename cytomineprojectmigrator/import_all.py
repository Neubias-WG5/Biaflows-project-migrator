# -*- coding: utf-8 -*-

# * Copyright (c) 2009-2018. Authors: see NOTICE file.
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# *      http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import sys
from argparse import ArgumentParser

from cytomine import Cytomine

from cytomineprojectmigrator.importer import Importer

__author__ = "Rubens Ulysse <urubens@uliege.be>"

if __name__ == '__main__':
    parser = ArgumentParser(prog="Cytomine Project Importer")
    parser.add_argument('--host', help="The Cytomine host on which project is imported.")
    parser.add_argument('--host_upload', help="The Cytomine host on which images are uploaded.")
    parser.add_argument('--public_key', help="The Cytomine public key used to import the project. "
                                             "The underlying user has to be a Cytomine administrator.")
    parser.add_argument('--private_key', help="The Cytomine private key used to import the project. "
                                              "The underlying user has to be a Cytomine administrator.")
    parser.add_argument('--project_path', default="", help="The base path where the project archive is stored.")
    # TODO: other options
    params, other = parser.parse_known_args(sys.argv[1:])

    with Cytomine(params.host, params.public_key, params.private_key) as _:
        options = {k:v for (k,v) in vars(params).items() if k.startswith('without')}

        for file in os.listdir(params.project_path):
            abs_path = os.path.join(params.project_path, file)
            if os.path.isdir(abs_path):
                print(abs_path)
                importer = Importer(params.host_upload, abs_path, **options)
                importer.run()