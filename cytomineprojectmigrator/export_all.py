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

import sys
from argparse import ArgumentParser

from cytomine import Cytomine
from cytomine.models import ProjectCollection

from cytomineprojectmigrator.exporter import Exporter

__author__ = "Rubens Ulysse <urubens@uliege.be>"

if __name__ == '__main__':
    parser = ArgumentParser(prog="Cytomine All Projects Exporter")
    parser.add_argument('--host', help="The Cytomine host from which projects zre exported.")
    parser.add_argument('--public_key', help="The Cytomine public key used to export the projects. "
                                             "The underlying user has to be a manager of the exported projects.")
    parser.add_argument('--private_key', help="The Cytomine private key used to export the projects. "
                                              "The underlying user has to be a manager of the exported projects.")
    parser.add_argument('--make_archive', default=True, help="Make an archive for the exported projects.")
    parser.add_argument('--working_path', default="", help="The base path where the generated archive will be stored.")
    parser.add_argument('--anonymize', default=False, help="Anonymize users in the projects.")
    parser.add_argument('--without_image_download', default=False, help="Do not download images but export image metadata.")
    parser.add_argument('--without_user_annotations', default=False, help="Do not export user annotations.")
    parser.add_argument('--without_metadata', default=False, help="Do not export any metadata.")
    parser.add_argument('--without_annotation_metadata', default=True, help="Do not export annotation metadata "
                                                                            "(speed up processing).")
    params, other = parser.parse_known_args(sys.argv[1:])

    with Cytomine(params.host, params.public_key, params.private_key) as _:
        Cytomine.get_instance().open_admin_session()
        options = {k:v for (k,v) in vars(params).items() if k.startswith('without') or k == 'anonymize'}

        for project in ProjectCollection().fetch():
            exporter = Exporter(params.working_path, project.id, **options)
            exporter.run()
            if params.make_archive:
                exporter.make_archive()

        Cytomine.get_instance().close_admin_session()