# -*- coding: utf-8 -*-

# * Copyright (c) 2009-2019. Authors: see NOTICE file.
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

import copy
import json
import os
import logging
import random
import shutil
import string
import sys
import tarfile
import time
from argparse import ArgumentParser

import requests
from cytomine import Cytomine
from cytomine.models import OntologyCollection, TermCollection, User, RelationTerm, ProjectCollection, \
    StorageCollection, AbstractImageCollection, ImageInstance, ImageInstanceCollection, AbstractImage, UserCollection, \
    Ontology, Project, Term, AnnotationCollection, Annotation, Property, Model, AttachedFile, Description, \
    ImageGroupCollection, ImageGroup, ImageSequenceCollection, ImageSequence, DisciplineCollection
from cytomine.models.image import SliceInstanceCollection, SliceInstance
from joblib import Parallel, delayed

__author__ = "Rubens Ulysse <urubens@uliege.be>"


def find_first(l):
    return l[0] if len(l) > 0 else None


def random_string(length=10):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))


def connect_as(user=None, open_admin_session=False):
    public_key = None
    private_key = None

    if hasattr(user, "publicKey") and user.publicKey:
        public_key = user.publicKey

    if hasattr(user, "privateKey") and user.privateKey:
        private_key = user.privateKey

    if not public_key or not private_key:
        keys = user.keys()
        public_key, private_key = keys["publicKey"], keys["privateKey"]

    Cytomine.get_instance().set_credentials(public_key, private_key)
    if open_admin_session:
        Cytomine.get_instance().open_admin_session()
    return Cytomine.get_instance().current_user


class Importer:
    def __init__(self, host_upload, working_path, with_original_date=False):
        self.host_upload = host_upload
        self.with_original_date = with_original_date
        self.id_mapping = {}

        self.working_path = working_path

        self.with_userannotations = False
        self.with_images = False

        self.super_admin = None

    def run(self):
        self.super_admin = Cytomine.get_instance().current_user
        connect_as(self.super_admin, True)

        users = UserCollection().fetch()
        users_json = [f for f in os.listdir(self.working_path) if f.endswith(".json") and f.startswith("user-collection")][0]
        remote_users = UserCollection()
        for u in json.load(open(os.path.join(self.working_path, users_json))):
            remote_users.append(User().populate(u))

        roles = ["project_manager", "project_contributor", "ontology_creator"]
        if self.with_images:
            roles += ["image_creator", "image_reviewer"]

        if self.with_userannotations:
            roles += ["userannotation_creator", "userannotationterm_creator"]

        roles = set(roles)
        remote_users = [u for u in remote_users if len(roles.intersection(set(u.roles))) > 0]

        for remote_user in remote_users:
            user = find_first([u for u in users if u.username == remote_user.username])
            if not user:
                user = copy.copy(remote_user)
                if not user.password:
                    user.password = random_string(8)
                if not self.with_original_date:
                    user.created = None
                    user.updated = None
                user.save()
            self.id_mapping[remote_user.id] = user.id

        # --------------------------------------------------------------------------------------------------------------
        logging.info("1/ Import ontology and terms")
        """
        Import the ontology with terms and relation terms that are stored in pickled files in working_path.
        If the ontology exists (same name and same terms), the existing one is used.
        Otherwise, an ontology with an available name is created with new terms and corresponding relationships.
        """
        ontologies = OntologyCollection().fetch()
        ontology_json = [f for f in os.listdir(self.working_path) if f.endswith(".json") and f.startswith("ontology")][0]
        remote_ontology = Ontology().populate(json.load(open(os.path.join(self.working_path, ontology_json))))
        remote_ontology.name = remote_ontology.name.strip()

        terms = TermCollection().fetch()
        terms_json = [f for f in os.listdir(self.working_path) if f.endswith(".json") and f.startswith("term-collection")]
        remote_terms = TermCollection()
        if len(terms_json) > 0:
            for t in json.load(open(os.path.join(self.working_path, terms_json[0]))):
                remote_terms.append(Term().populate(t))

        def ontology_exists():
            compatible_ontology = find_first([o for o in ontologies if o.name == remote_ontology.name.strip()])
            if compatible_ontology:
                set1 = set((t.name, t.color) for t in terms if t.ontology == compatible_ontology.id)
                difference = [term for term in remote_terms if (term.name, term.color) not in set1]
                if len(difference) == 0:
                    return True, compatible_ontology
                return False, None
            else:
                return True, None

        i = 1
        remote_name = remote_ontology.name
        found, existing_ontology = ontology_exists()
        while not found:
            remote_ontology.name = "{} ({})".format(remote_name, i)
            found, existing_ontology = ontology_exists()
            i += 1

        # SWITCH to ontology creator user
        connect_as(User().fetch(self.id_mapping[remote_ontology.user]))
        if not existing_ontology:
            ontology = copy.copy(remote_ontology)
            ontology.user = self.id_mapping[remote_ontology.user]
            if not self.with_original_date:
                ontology.created = None
                ontology.updated = None
            ontology.save()
            self.id_mapping[remote_ontology.id] = ontology.id
            logging.info("Ontology imported: {}".format(ontology))

            for remote_term in remote_terms:
                logging.info("Importing term: {}".format(remote_term))
                term = copy.copy(remote_term)
                term.ontology = self.id_mapping[term.ontology]
                term.parent = None
                if not self.with_original_date:
                    term.created = None
                    term.updated = None
                term.save()
                self.id_mapping[remote_term.id] = term.id
                logging.info("Term imported: {}".format(term))

            remote_relation_terms = [(term.parent, term.id) for term in remote_terms]
            for relation in remote_relation_terms:
                parent, child = relation
                if parent:
                    rt = RelationTerm(self.id_mapping[parent], self.id_mapping[child]).save()
                    logging.info("Relation term imported: {}".format(rt))
        else:
            self.id_mapping[remote_ontology.id] = existing_ontology.id

            ontology_terms = [t for t in terms if t.ontology == existing_ontology.id]
            for remote_term in remote_terms:
                self.id_mapping[remote_term.id] = find_first([t for t in ontology_terms if t.name == remote_term.name]).id

            logging.info("Ontology already encoded: {}".format(existing_ontology))

        # SWITCH USER
        connect_as(self.super_admin, True)

        # --------------------------------------------------------------------------------------------------------------
        logging.info("2/ Import project")
        """
        Import the project (i.e. the Cytomine Project domain) stored in pickled file in working_path.
        If a project with the same name already exists, append a (x) suffix where x is an increasing number.
        """
        disciplines = DisciplineCollection().fetch()

        projects = ProjectCollection().fetch()
        project_json = [f for f in os.listdir(self.working_path) if f.endswith(".json") and f.startswith("project")][0]
        remote_project = Project().populate(json.load(open(os.path.join(self.working_path, project_json))))
        remote_project.name = remote_project.name.strip()

        def available_name():
            i = 1
            existing_names = [o.name for o in projects]
            new_name = project.name
            while new_name in existing_names:
                new_name = "{} ({})".format(project.name, i)
                i += 1
            return new_name

        project = copy.copy(remote_project)
        project.name = available_name()
        project.discipline = find_first([d.id for d in disciplines if d.name == project.disciplineName])
        project.ontology = self.id_mapping[project.ontology]
        project_contributors = [u for u in remote_users if "project_contributor" in u.roles]
        project.users = [self.id_mapping[u.id] for u in project_contributors]
        project_managers = [u for u in remote_users if "project_manager" in u.roles]
        project.admins = [self.id_mapping[u.id] for u in project_managers]
        if not self.with_original_date:
            project.created = None
            project.updated = None
        project.save()
        self.id_mapping[remote_project.id] = project.id
        logging.info("Project imported: {}".format(project))

        # --------------------------------------------------------------------------------------------------------------
        logging.info("3/ Import images")
        storages = StorageCollection(all=True).fetch()
        abstract_images = AbstractImageCollection().fetch()

        groups_json = [f for f in os.listdir(self.working_path) if f.endswith(".json")
                       and f.startswith("imagegroup-collection")]
        remote_groups = ImageGroupCollection()
        if len(groups_json) > 0:
            for i in json.load(open(os.path.join(self.working_path, groups_json[0]))):
                remote_groups.append(ImageGroup().populate(i))

        if len(remote_groups) > 0:
            # Get image sequences.
            sequences_json = [f for f in os.listdir(self.working_path) if f.endswith(".json")
                           and f.startswith("imagesequence-collection")]
            remote_sequences = ImageSequenceCollection()
            if len(sequences_json) > 0:
                for i in json.load(open(os.path.join(self.working_path, sequences_json[0]))):
                    remote_sequences.append(ImageSequence().populate(i))

            remote_groups_dict = {}
            for remote_group in remote_groups:
                group = copy.copy(remote_group)

                # Fix old image name due to urllib3 limitation
                remote_group.name = bytes(remote_group.name, 'utf-8').decode('ascii', 'ignore')
                if remote_group.name not in remote_groups_dict.keys():
                    remote_groups_dict[remote_group.name] = [remote_group]
                else:
                    remote_groups_dict[remote_group.name].append(remote_group)
                logging.info("Importing image (multidimensional): {}".format(remote_group))

                # Find uploader
                first_seq = find_first([s for s in remote_sequences if s.imageGroup == remote_group.id])

                # SWITCH user to image creator user
                connect_as(User().fetch(self.id_mapping[first_seq.model['user']]))
                # Get its storage
                storage = find_first([s for s in storages if s.user == Cytomine.get_instance().current_user.id])
                if not storage:
                    storage = storages[0]

                logging.info("== New image starting to upload & deploy")
                filename = os.path.join(self.working_path, "imagegroups", group.name.replace("/", "-"))
                Cytomine.get_instance().upload_image(self.host_upload, filename, storage.id,
                                                     self.id_mapping[remote_project.id])
                time.sleep(0.8)

                # SWITCH USER
                connect_as(self.super_admin, True)

            # Waiting for all images...
            n_new_groups = -1
            count = 0
            new_groups = None
            while n_new_groups != len(remote_groups) and count < len(remote_groups) * 5:
                new_groups = ImageGroupCollection().fetch_with_filter("project", self.id_mapping[remote_project.id])
                n_new_groups = len(new_groups)
                if count > 0:
                    time.sleep(5)
                count = count + 1
            print("All images have been deployed. Fixing groups ...")

            for new_group in new_groups:
                remote_group = remote_groups_dict[new_group.name].pop()
                if self.with_original_date:
                    new_group.created = remote_group.created
                    new_group.updated = remote_group.updated
                new_group.update()
                self.id_mapping[remote_group.id] = new_group.id

            print("All image groups have been fixed.")
        else:
            images_json = [f for f in os.listdir(self.working_path) if f.endswith(".json")
                           and f.startswith("imageinstance-collection")]
            slices_json = [f for f in os.listdir(self.working_path) if f.endswith(".json")
                           and f.startswith("sliceinstance-collection")]
            remote_images = ImageInstanceCollection()
            remote_slices = SliceInstanceCollection()
            if len(images_json) > 0:
                for i in json.load(open(os.path.join(self.working_path, images_json[0]))):
                    remote_images.append(ImageInstance().populate(i))

                for i in json.load(open(os.path.join(self.working_path, slices_json[0]))):
                    remote_slices.append(SliceInstance().populate(i))


            remote_images_dict = {}

            for remote_image in remote_images:
                image = copy.copy(remote_image)

                # Fix old image name due to urllib3 limitation
                remote_image.originalFilename = bytes(remote_image.originalFilename, 'utf-8').decode('ascii', 'ignore')
                if remote_image.originalFilename not in remote_images_dict.keys():
                    remote_images_dict[remote_image.originalFilename] = [remote_image]
                else:
                    remote_images_dict[remote_image.originalFilename].append(remote_image)
                logging.info("Importing image: {}".format(remote_image))

                # SWITCH user to image creator user
                connect_as(User().fetch(self.id_mapping[remote_image.user]))
                # Get its storage
                storage = find_first([s for s in storages if s.user == Cytomine.get_instance().current_user.id])
                if not storage:
                    storage = storages[0]

                # Check if image is already in its storage
                abstract_image = find_first([ai for ai in abstract_images
                                             if ai.originalFilename == remote_image.originalFilename
                                             and ai.width == remote_image.width
                                             and ai.height == remote_image.height
                                             and ai.physicalSizeX == remote_image.physicalSizeX])
                if abstract_image:
                    logging.info("== Found corresponding abstract image. Linking to project.")
                    ImageInstance(abstract_image.id, self.id_mapping[remote_project.id]).save()
                else:
                    logging.info("== New image starting to upload & deploy")
                    filename = os.path.join(self.working_path, "images", image.originalFilename.replace("/", "-"))
                    Cytomine.get_instance().upload_image(self.host_upload, filename, storage.id,
                                                         self.id_mapping[remote_project.id])
                    time.sleep(0.8)

                # SWITCH USER
                connect_as(self.super_admin, True)

            # Waiting for all images...
            n_new_images = -1
            new_images = None
            count = 0
            while n_new_images != len(remote_images) and count < len(remote_images) * 5:
                new_images = ImageInstanceCollection().fetch_with_filter("project", self.id_mapping[remote_project.id])
                n_new_images = len(new_images)
                if count > 0:
                    time.sleep(5)
                count = count + 1
            print("All images have been deployed. Fixing image-instances...")

            # Fix image instances meta-data:
            for new_image in new_images:
                remote_image = remote_images_dict[new_image.originalFilename].pop()
                if self.with_original_date:
                    new_image.created = remote_image.created
                    new_image.updated = remote_image.updated
                new_image.reviewStart = remote_image.reviewStart if hasattr(remote_image, 'reviewStart') else None
                new_image.reviewStop = remote_image.reviewStop if hasattr(remote_image, 'reviewStop') else None
                new_image.reviewUser = self.id_mapping[remote_image.reviewUser] if hasattr(remote_image, 'reviewUser') and remote_image.reviewUser else None
                new_image.instanceFilename = remote_image.instanceFilename
                new_image.update()
                self.id_mapping[remote_image.id] = new_image.id
                self.id_mapping[remote_image.baseImage] = new_image.baseImage

                new_abstract = AbstractImage().fetch(new_image.baseImage)
                if self.with_original_date:
                    new_abstract.created = remote_image.created
                    new_abstract.updated = remote_image.updated
                if new_abstract.physicalSizeX is None:
                    new_abstract.physicalSizeX = remote_image.physicalSizeX
                if new_abstract.magnification is None:
                    new_abstract.magnification = remote_image.magnification
                new_abstract.update()

                slices = SliceInstanceCollection().fetch_with_filter("imageinstance", new_image.id)
                for remote_slice in [s for s in remote_slices if s.image == remote_image.id]:
                    new_slice = find_first([s for s in slices if s.channel == remote_slice.channel
                                            and s.zStack == remote_slice.zStack and s.time == remote_slice.time])
                    if new_slice:
                        self.id_mapping[remote_slice.id] = new_slice.id

            print("All image-instances have been fixed.")

        # --------------------------------------------------------------------------------------------------------------
        logging.info("4/ Import user annotations")
        annots_json = [f for f in os.listdir(self.working_path) if f.endswith(".json") and f.startswith("user-annotation-collection")]
        remote_annots = AnnotationCollection()
        if len(annots_json) > 0:
            for a in json.load(open(os.path.join(self.working_path, annots_json[0]))):
                remote_annots.append(Annotation().populate(a))

        def _add_annotation(remote_annotation, id_mapping, with_original_date):
            if remote_annotation.project not in id_mapping.keys() \
                    or remote_annotation.image not in id_mapping.keys():
                return

            annotation = copy.copy(remote_annotation)
            annotation.project = id_mapping[remote_annotation.project]
            annotation.image = id_mapping[remote_annotation.image]
            annotation.slice = id_mapping[remote_annotation.slice]
            annotation.user = id_mapping[remote_annotation.user]
            annotation.term = [id_mapping[t] for t in remote_annotation.term]
            if not with_original_date:
                annotation.created = None
                annotation.updated = None
            annotation.save()

        for user in [u for u in remote_users if "userannotation_creator" in u.roles]:
            remote_annots_for_user = [a for a in remote_annots if a.user == user.id]
            # SWITCH to annotation creator user
            connect_as(User().fetch(self.id_mapping[user.id]))
            Parallel(n_jobs=-1, backend="threading")(delayed(_add_annotation)
                                                     (remote_annotation, self.id_mapping, self.with_original_date)
                                                     for remote_annotation in remote_annots_for_user)

            # SWITCH back to admin
            connect_as(self.super_admin, True)

        # --------------------------------------------------------------------------------------------------------------
        logging.info("5/ Import metadata (properties, attached files, description)")
        obj = Model()
        obj.id = -1
        obj.class_ = ""

        properties_json = [f for f in os.listdir(self.working_path) if
                       f.endswith(".json") and f.startswith("properties")]
        for property_json in properties_json:
            for remote_prop in json.load(open(os.path.join(self.working_path, property_json))):
                prop = Property(obj).populate(remote_prop)
                prop.domainIdent = self.id_mapping[prop.domainIdent]
                prop.save()

        new_descriptions = []
        descriptions_json = [f for f in os.listdir(self.working_path) if f.endswith(".json") and f.startswith("description")]
        for description_json in descriptions_json:
            desc = Description(obj).populate(json.load(open(os.path.join(self.working_path, description_json))))
            desc_id = desc.id
            desc.domainIdent = self.id_mapping[desc.domainIdent]
            desc._object.class_ = desc.domainClassName
            desc._object.id = desc.domainIdent
            new_desc = desc.save()
            self.id_mapping[desc_id] = new_desc.id
            new_descriptions.append(new_desc)

        attached_file_id_mapping = {}
        attached_files_json = [f for f in os.listdir(self.working_path) if
                               f.endswith(".json") and f.startswith("attached-files")]
        for attached_file_json in attached_files_json:
            for remote_af in json.load(open(os.path.join(self.working_path, attached_file_json))):
                af = AttachedFile(obj).populate(remote_af)
                af.domainIdent = self.id_mapping[af.domainIdent]
                af.filename = os.path.join(self.working_path, "attached_files", af.filename)
                af_id = af.id
                af.id = None
                new_af = af.save()
                if new_af:
                    attached_file_id_mapping[af_id] = new_af.id
                else:
                    print("ERROR: attached file {}".format(remote_af))

        for description in new_descriptions:
            if "attachedfile/" in description.data:
                for (id, new_id) in attached_file_id_mapping.items():
                    description.data = description.data.replace("attachedfile/{}".format(id), "attachedfile/{}".format(new_id))
                description.update()


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

        if params.project_path.startswith("http://") or params.project_path.startswith("https://"):
            logging.info("Downloading from {}".format(params.project_path))
            response = requests.get(params.project_path, allow_redirects=True, stream=True)
            params.project_path = params.project_path[params.project_path.rfind("/") + 1 :]
            with open(params.project_path, "wb") as f:
                shutil.copyfileobj(response.raw, f)
                logging.info("Downloaded successfully.")

        if params.project_path.endswith(".tar.gz"):
            tar = tarfile.open(params.project_path, "r:gz")
            tar.extractall(os.path.dirname(params.project_path))
            tar.close()
            params.project_path = params.project_path[:-7]
        elif params.project_path.endswith(".tar"):
            tar = tarfile.open(params.project_path, "r:")
            tar.extractall(os.path.dirname(params.project_path))
            tar.close()
            params.project_path = params.project_path[:-4]

        importer = Importer(params.host_upload, params.project_path, **options)
        importer.run()