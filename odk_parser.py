

import requests
import re
import os
import collections
import logging
import traceback
import json
import copy
import subprocess
import hashlib

from datetime import datetime
from collections import defaultdict, OrderedDict
from django.db import connections

from raven import Client
from django.conf import settings
from django.forms.models import model_to_dict
from django.db import connection, transaction, IntegrityError
from django.core.paginator import Paginator
from django.core.exceptions import FieldDoesNotExist
from requests.exceptions import ConnectionError
from django.http import HttpRequest

from .terminal_output import Terminal
from .excel_writer import ExcelWriter
from .models import ODKForm, RawSubmissions, FormViews, ViewsData, ViewTablesLookup, DictionaryItems, FormMappings, ProcessingErrors, ODKFormGroup, SystemSettings
import six
from six.moves import range
from six.moves import zip

terminal = Terminal()

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO'),
        },
    },
}
logger = logging.getLogger('ODKForms')
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)
request = HttpRequest()


class OdkParser():
    def __init__(self, ona_user=None, ona_password=None, ona_token=None):
        terminal.tprint("Initializing the core ODK parser", 'ok')

        global sentry
        sentry = Client(settings.SENTRY_DSN)

        # ona api end points
        self.api_all_forms = 'api/v1/forms'
        self.form_data = 'api/v1/data/'
        self.form_stats = 'api/v1/stats/submissions/'
        self.form_rep = 'api/v1/forms/'
        self.media = 'api/v1/media'

        if ona_token is None:
            # load the global ona settings
            self.load_ona_settings()
        else:
            self.ona_user = ona_user
            self.ona_password = ona_password
            self.ona_api_token = ona_token
            self.ona_url = settings.ONADATA_URL

        # load the destination database
        self.load_mapped_connection()

        # save the cleaners for later user
        # @todo Save as database settings
        self.cleaners = {
            'c1s1q8_Country_name': {
                '^sundan|sudan': 'Sudan',
                '^b.*nin|ben': 'Benin',
                '254$': 'Kenya',
                '243$': 'D. R. Congo',
                '^madag': 'Madagascar',
                '233$': 'Ghana',
                '251$': 'Ethiopia',
                '20$': 'Egypt',
                '258$': 'Mozambique'
            }
        }

    def load_ona_settings(self):
        # if the ona settings have been saved, load them here for later use
        ona_settings = SystemSettings.objects.filter(setting_key__contains='ona_')

        for setting in ona_settings:
            setattr(self, setting.setting_key, setting.setting_value)

    def load_defined_settings(self):
        """
        Load the defined settings in the database to be system variables
        """
        sys_settings = SystemSettings.objects.all()
        for setting in sys_settings:
            setattr(self, setting.setting_key, setting.setting_value)

    def load_mapped_connection(self):
        all_settings = self.get_all_settings()
        if 'dest_db_host_name' in all_settings:
            newDatabase = {}
            newDatabase["id"] = 'mapped'            # just something unique
            newDatabase['ENGINE'] = 'django.db.backends.mysql'
            newDatabase['NAME'] = all_settings['dest_db_name']
            newDatabase['USER'] = all_settings['dest_db_username']
            newDatabase['PASSWORD'] = all_settings['dest_db_password']
            newDatabase['HOST'] = all_settings['dest_db_host_name']
            newDatabase['PORT'] = all_settings['dest_db_port']
            connections.databases['mapped'] = newDatabase
        else:
            terminal.tprint("The mapped database connection is not set, mappings will not be possible downstream", 'fail')

    def are_ona_settings_saved(self):
        """Summary
        Returns:
            TYPE: Description
        """
        ona_settings = SystemSettings.objects.filter(setting_key__contains='ona_')
        return False if len(ona_settings) == 0 else True

    def get_all_forms(self):
        """
        Get all the forms belonging to the current project
        """

        to_return = []
        to_return.append({'title': 'Select One', 'id': '-1'})
        # check whether the form is already saved in the database
        try:
            all_forms = ODKForm.objects.all()
            for form in all_forms:
                to_return.append({'title': form.form_name, 'id': form.form_id})
        except Exception as e:
            sentry.captureException()
            terminal.tprint(str(e), 'fail')

        terminal.tprint(json.dumps(to_return), 'warn')
        return to_return

    def get_value_from_dictionary(self, t_key):
        try:
            query = """
                SELECT t_value from dictionary_items where t_key = '%s'
            """ % t_key
            with connection.cursor() as cursor:
                cursor.execute(query)
                t_value = cursor.fetchall()
            return t_value[0][0]
        except Exception as e:
            logging.error("Couldn't find the value for the key '%s' in the dictionary. %s" % (t_key, str(e)))
            terminal.tprint("Couldn't find the value for the key '%s' in the dictionary. %s" % (t_key, str(e)), 'fail')
            sentry.captureException()
            return "Unknown (%s)" % t_key

    def refresh_forms(self, process_structure=False, auto_create_form_group=False):
        """
        Refresh the list of forms in the database
        """
        to_return = []
        to_return.append({'title': 'Select One', 'id': '-1'})

        try:
            url = "%s/%s" % (self.ona_url, self.api_all_forms)
            all_forms = self.process_curl_request(url)
            # terminal.tprint(json.dumps(all_forms), 'fail')
            if all_forms is None:
                print(("Error while executing the API request %s" % url))
                return
        except ConnectionError as e:
            terminal.tprint("We don't have a connection to the ONA server, revert to the saved forms..", 'debug')
            sentry.captureMessage("I cannot connect to the ONA server. I will revert to the saved forms", level='warning', extra={'messasge': str(e)})
            saved_forms = ODKForm.objects.all()
            for form in saved_forms:
                to_return.append({'title': form.form_name, 'id': form.form_id, 'full_id': form.full_form_id})
            return to_return
        except Exception as e:
            print(e)
            # sentry.captureException()
            terminal.tprint(str(e), 'fail')
            raise Exception('There was an error while fetching new forms from the database...')

        for form in all_forms:
            # check whether the form is already saved in the database
            try:
                saved_form = ODKForm.objects.get(full_form_id=form['id_string'])
                terminal.tprint("The form '%s' is already saved in the database" % saved_form.form_name, 'ok')
                to_return.append({'title': saved_form.form_name, 'id': saved_form.form_id, 'full_id': saved_form.full_form_id})
            except ODKForm.DoesNotExist as e:
                # this form is not saved in the database, so save it
                terminal.tprint("The form '%s' is not in the database, saving it" % form['id_string'], 'warn')
                if auto_create_form_group:
                    form_group = self.auto_create_form_group(form['id_string'])
                    if form_group is None:
                        # we have been asked to create a group if it doesn't exist but encountered an error! I just cant go on, I refuse
                        raise Exception("There was an error while creating an automatic form group, which was needed!")
                else:
                    form_group = None

                try:
                    cur_form = ODKForm(
                        form_id=form['formid'],
                        form_group=form_group,
                        form_name=form['title'],
                        full_form_id=form['id_string'],
                        auto_update=False,
                        is_source_deleted=False,
                        no_submissions=form['num_of_submissions'],
                        is_active=form['downloadable'],
                        datetime_published=form['date_created'],
                        latest_upload=form['last_updated_at']
                    )
                    cur_form.publish()
                except FieldDoesNotExist:
                    cur_form = ODKForm(
                        form_id=form['formid'],
                        form_group=form_group,
                        form_name=form['title'],
                        full_form_id=form['id_string'],
                        auto_update=False,
                        is_source_deleted=False
                    )
                    cur_form.publish()
                except:
                    raise

                if process_structure:
                    # we to process and save the form structure
                    # we need to have the cur_form_group set for this operation
                    if form_group is None:
                        raise Exception("Refusing to process the form structure for '%s' since the form group is not defined. This process will fail downstream." % form['id_string'])

                    self.cur_form_group = form_group.id
                    self.get_form_structure_from_server(form['formid'])

                to_return.append({'title': form['title'], 'id': form['formid']})
            except Exception as e:
                sentry.captureException()
                terminal.tprint(str(e), 'fail')

        return to_return

    def get_all_submissions(self, form_id, uuids=None, update_local_data=True):
        """
        Given a form id or the uuids, get all the submitted data
        
        Args:
            form_id (TYPE): Description
            uuids (None, optional): Description
            update_local_data (bool, optional): Whether or not we should fetch new submissions from the ODK server
        
        Returns:
            TYPE: Description
        
        Raises:
            Exception: Description
        """
        try:
            # the form_id used in odk_forms and submissions is totally different
            odk_form = ODKForm.objects.get(form_id=form_id)

            if uuids is not None:
                terminal.tprint('\tWe are only interested in data already saved and we have their uuids: %s' % json.dumps(uuids), 'ok')
                # print odk_form.id
                # we are only interested in data already saved and we have their uuids
                submissions = RawSubmissions.objects.filter(uuid__in=uuids).filter(form_id=odk_form.id).values('raw_data')
                return submissions

            submissions = RawSubmissions.objects.filter(form_id=odk_form.id).values('raw_data')
            if update_local_data is False:
                terminal.tprint('\tWe dont need to update local database, so lets return what is already saved in the database', 'warn')
                return submissions

            submitted_instances = self.online_submissions_count(form_id)

            # check whether all the submissions from the db match the online submissions
            if submitted_instances is None:
                # There was an error while fetching the submissions, use 0 as submitted_instances
                submitted_instances = 0

            terminal.tprint('\t%s: Saved submissions "%d" vs Submitted submissions "%d"' % (odk_form.form_name, submissions.count(), submitted_instances), 'okblue')
            if submissions.count() == 0 and submitted_instances == 0:
                logger.info('There are no submissions to process')
                terminal.tprint('No submisions to process', 'fail')
                return None

            if submitted_instances > submissions.count():
                # we have some new submissions, so fetch them from the server and save them offline
                terminal.tprint("\tWe have some new submissions, so fetch them from the server and save them offline", 'info')
                # fetch the submissions and filter by submission time
                if settings.IS_DRY_RUN:
                    url = "%s/%s%d.json?start=1&limit=5&sort=%s" % (self.ona_url, self.form_data, form_id, '{"_submission_time":-1}')
                else:
                    url = "%s/%s%d.json?sort=%s" % (self.ona_url, self.form_data, form_id, '{"_submission_time":-1}')

                # url = "%s%s%d.json?fields=[\"_uuid\", \"_id\"]" % (self.ona_url, self.form_data, form_id)
                submission_uuids = self.process_curl_request(url)

                if settings.IS_DRY_RUN:
                    subm_count = 0
                for uuid in submission_uuids:
                    # obey the debug setting
                    if settings.IS_DRY_RUN:
                        subm_count = subm_count + 1
                        if subm_count > settings.DRY_RUN_RECORDS:
                            terminal.tprint("\tWe have downloaded our maximum number of submissions under dry ran settings", 'info')
                            break

                    # check if the current uuid is saved in the database
                    # print odk_form.id
                    # print uuid['_uuid']
                    cur_submission = RawSubmissions.objects.filter(form_id=odk_form.id, uuid=uuid['_uuid'])
                    if cur_submission.count() == 0:
                        # the current submission is not saved in the database, so fetch and save it...
                        url = "%s/%s%d/%s" % (self.ona_url, self.form_data, form_id, uuid['_id'])
                        submission = self.process_curl_request(url)
                        # terminal.tprint(json.dumps(submission), 'warn')

                        t_submission = RawSubmissions(
                            form_id=odk_form.id,
                            # it seems some submissions don't have a uuid returned with the submission. Use our previous uuid
                            uuid=uuid['_uuid'],
                            submission_time=submission['_submission_time'],
                            raw_data=submission
                        )
                        t_submission.publish()
                    else:
                        # the current submission is already saved, so stop the processing
                        # terminal.tprint("The current submission is already saved, implying that all submissions have been processed, so stop the processing!", 'okblue')
                        continue

                # just check if all is now ok
                submissions = RawSubmissions.objects.filter(form_id=odk_form.id).order_by('submission_time').values('raw_data')
                if submissions.count() != submitted_instances:
                    if settings.IS_DRY_RUN:
                        terminal.tprint("\tThe system is under development, no need to confirm number of counts", 'debug')
                    else:
                        # ok, still the processing is not complete... shout!
                        terminal.tprint("\tEven after processing submitted responses for '%s', the tally doesn't match (%d vs %d)!" % (odk_form.form_name, submissions.count(), submitted_instances), 'error')
                else:
                    terminal.tprint("\tSubmissions for '%s' successfully updated." % odk_form.form_name, 'info')
            else:
                terminal.tprint("\tAll submissions for '%s' are already saved in the database" % odk_form.form_name, 'info')

        except Exception as e:
            logger.error(str(e))
            terminal.tprint(str(e), 'error')
            sentry.captureException()
            raise Exception(str(e))

        return submissions

    def online_submissions_count(self, form_id):
        # given a form id, process the number of submitted instances
        # terminal.tprint("\tComputing the number of submissions of the form with id '%s'" % form_id, 'info')
        url = "%s/%s%d?%s" % (self.ona_url, self.form_stats, form_id, "group=&name=time")
        try:
            stats = self.process_curl_request(url)
        except ConnectionError:
            # in case of a connection error return None
            return None

        if stats is None:
            logger.error("Error while fetching the number of submissions")
            return None

        submissions_count = 0
        for stat in stats:
            submissions_count += int(stat['count'])

        return submissions_count

    def get_form_structure_as_json(self, form_id):
        """
        check whether the form structure is already saved in the DB
        """
        terminal.tprint("Fetching form with id %d" % form_id, 'warn')
        try:
            cur_form = ODKForm.objects.get(form_id=form_id)

            if cur_form.form_group_id is not None:
                cur_form_group = ODKFormGroup.objects.get(id=cur_form.form_group_id)
                self.cur_form_group = cur_form_group.group_name
            else:
                self.cur_form_group = 'Undefined'

            # check if the structure exists
            # FORCE A STRUCTURE REGENERATION
            # if cur_form.structure is not None:
            if cur_form.structure is None:
                # we don't have the structure, so fetch, process and save the structure
                terminal.tprint("\tThe form '%s' doesn't have a saved structure, so lets fetch it and add it" % cur_form.form_name, 'warn')
                (processed_nodes, structure) = self.get_form_structure_from_server(form_id)
                # terminal.tprint(json.dumps(structure), 'okblue')
                if structure is not None:
                    cur_form.structure = structure
                    # cur_form.structure = json.dumps(structure)
                    cur_form.processed_structure = processed_nodes
                    cur_form.publish()
                else:
                    raise Exception("There was an error in fetching the selected form and it is not yet saved in the database.")
            else:
                terminal.tprint("\tFetching the form's '%s' structure from the database" % cur_form.form_name, 'okblue')
                # We are expecting the processed_structure as a list of json and the structure as a json
                struct_type = self.determine_type(cur_form.processed_structure)

                terminal.tprint('The processed structure type is -- %s' % struct_type, 'fail')
                if(struct_type is 'is_string'):
                    processed_nodes = cur_form.processed_structure

                    # I am gambling that the following are only needed when a string is fetched from the database
                    m = re.findall(r"^'(.+)'$", processed_nodes)
                    processed_nodes = json.loads(m[0])
                elif(struct_type is 'is_list'):
                    processed_nodes = cur_form.processed_structure
                else:
                    processed_nodes = (cur_form.processed_structure)

                # force a re-extraction of the nodes (2019-06-14 not clear why we need to do this.... commenting out this block for now)
                # self.cur_node_id = 0
                # self.cur_form_id = form_id
                # self.repeat_level = 0
                # self.all_nodes = []
                # self.top_node = {"name": "Main", "label": "Top Level", "parent_id": -1, "type": "top_level", "id": 0}
                # terminal.tprint(json.dumps(cur_form.structure), 'okblue')
                # When saving the json strings to the database, some crazy string manipulation is happening.
                # In case we have escaped quotes, they are further escaped, hence we need to strip them in addition to the single quotes stored at the begining and at the end
                # Took a cool 2 days of head scratching and excessive hair loss
                # structure = json.loads(cur_form.structure.strip('\'').replace('\\\\"', ''))
                # self.top_level_hierarchy = self.extract_repeating_groups(structure, 0)
        except IntegrityError as e:
            # We can live with this
            terminal.tprint(str(e), 'fail')
        except TypeError as e:
            # Can we live with this???
            print((traceback.format_exc()))
            terminal.tprint(str(e), 'fail')
            # terminal.tprint(json.dumps(processed_nodes), 'fail')
            terminal.tprint('Can we live with this (%s) error???' % str(e), 'ok')
        except Exception as e:
            print((traceback.format_exc()))
            logger.debug(str(e))
            terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

        # the processed nodes to be return should be as a list nodes
        return processed_nodes

    def get_form_structure_from_server(self, form_id):
        """
        Get the structure of the current form
        """
        try:
            url = "%s/%s%d/form.json" % (self.ona_url, self.form_rep, form_id)
            terminal.tprint("Fetching the form structure for form with id = %d" % form_id, 'header')
            form_structure = self.process_curl_request(url)

            if form_structure is None:
                return (None, None)

            self.cur_node_id = 0
            self.cur_form_id = form_id
            self.repeat_level = 0
            self.all_nodes = []
            self.top_node = {"name": "Main", "label": "Top Level", "parent_id": -1, "type": "top_level", "id": 0}

            # initialize a current section variable if we are to group them
            self.cur_section = None

            self.top_level_hierarchy = self.extract_repeating_groups(form_structure, 0, True)
            self.all_nodes.insert(0, self.top_node)
            terminal.tprint("Processed %d group nodes" % self.cur_node_id, 'warn')

            # print all the json for creating the tree
            # terminal.tprint(json.dumps(self.all_nodes), 'warn')
            return self.all_nodes, form_structure
        except Exception as e:
            sentry.captureException()
            terminal.tprint(str(e), 'fail')
            raise

    def extract_repeating_groups(self, nodes, parent_id, use_sections=False):
        """
        Process a node and get the repeating groups
        """
        cur_node = []
        node_type = self.determine_type(nodes)
        # terminal.tprint(json.dumps(nodes), 'debug')
        # print(nodes) 
        if node_type == 'is_string':
            nodes = json.loads(nodes)
        for node in nodes['children']:
            # print('Level %d' % self.repeat_level)
            if 'type' in node:
                if 'label' in node:
                    (node_label, locale) = self.process_node_label(node)
                else:
                    # terminal.tprint("%s missing label. Using name('%s') instead" % (node['type'], node['name']), 'warn')
                    node_label = node['name']

                if use_sections:
                    (cur_section, section_name) = self.get_current_section(node_label)
                    if cur_section is not None and cur_section != self.cur_section:
                        # we need to add a new top section for the next nodes
                        terminal.tprint("\tAdding a new section(%s) from '%s' to the stack" % (cur_section, node_label), 'debug')
                        self.cur_node_id += 1
                        t_node = {'id': self.cur_node_id, 'parent_id': 0, 'type': 'new_section', 'label': "%s - %s" % (cur_section.upper(), section_name), 'name': cur_section}
                        self.all_nodes.append(t_node)
                        self.cur_section = cur_section
                        # add the upcoming nodes to the newly created node
                        parent_id = self.cur_node_id

                if node['type'] == 'repeat' or node['type'] == 'group':
                    # terminal.tprint("\nProcessing %s" % node_label, 'okblue')
                    # only add a node when we are dealing with a repeat
                    if node['type'] == 'repeat':
                        self.cur_node_id += 1
                        t_node = {'id': self.cur_node_id, 'parent_id': parent_id, 'type': node['type'], 'label': node_label, 'name': node['name'], 'items': []}
                    else:
                        t_node = None

                    if 'children' in node:
                        # terminal.tprint("\t%s-%s has %d children" % (node['type'], node_label, len(node['children'])), 'ok')
                        # determine parent_id. If we are in a group, pass the current parent_id, else pass the cur_node_id
                        if node['type'] == 'repeat':
                            t_parent_id = self.cur_node_id
                            self.repeat_level += 1
                        else:
                            t_parent_id = parent_id
                        child_node = self.extract_repeating_groups(node, t_parent_id, use_sections)

                        if len(child_node) != 0:
                            if t_node is None:
                                # we have something to save yet it wasn't wrapped in a repeat initially
                                # terminal.tprint("\tWe have something to save yet it wasn't wrapped in a repeat initially", 'warn')
                                # self.cur_node_id += 1
                                # terminal.tprint("\t%d:%s--%s" % (self.cur_node_id, node['type'], json.dumps(child_node[0])), 'warn')
                                t_node = child_node[0]
                            else:
                                t_node['items'].append(child_node[0])
                    # else:
                        # this node has no children. If its a top level node, include it in the top level page
                    #    if self.repeat_level == 0:

                    if t_node is not None and node['type'] == 'repeat':
                        if 'items' in t_node and len(t_node['items']) == 0:
                            del t_node['items']
                        cur_node.append(t_node)
                        # terminal.tprint("\t%d:%s--%s" % (self.cur_node_id, node['type'], json.dumps(t_node)), 'warn')
                        self.add_to_all_nodes(t_node)
                else:
                    # before anything, add this node to the dictionary
                    if node['type'] != 'calculate':
                        self.add_dictionary_items(node, node['type'])

                    # if self.repeat_level == 0:
                    self.cur_node_id += 1
                    # terminal.tprint("\tAdding a top node("+ node_label +") child. Should it really be added to the top node?", 'ok')
                    t_node = {'id': self.cur_node_id, 'parent_id': parent_id, 'type': node['type'], 'label': node_label, 'name': node['name']}
                    self.all_nodes.append(t_node)
            else:
                # we possibly have the options, so add them to the dictionary
                self.add_dictionary_items(node, 'choice')

        # should only be decremented when coming from a repeat group
        if(node['type'] == 'repeat'):
            self.repeat_level -= 1
        return cur_node

    def get_current_section(self, label):
        # get the current section and return it or return None if not found
        m = re.search('^(s\d+p\d+)', label, re.I)
        if m is not None:
            t = re.search("([\w\'\s']+)$", label, re.I)
            grp_name = t.group(1).strip().upper() if t is not None else ''
        return (m.group(1).lower(), grp_name) if m is not None and len(m.groups()) == 1 else (None, None)

    def add_dictionary_items(self, node, node_type, parent_node=None):
        # check if this key already exists
        dict_item = DictionaryItems.objects.filter(form_group=self.cur_form_group, parent_node=parent_node, t_key=node['name'])

        if dict_item.count() == 0:
            # terminal.tprint('\tSaving the node (%s)' % node_type, 'warn')
            # terminal.tprint('\t\t%s' % json.dumps(node), 'okblue')
            node_label = node['label'] if 'label' in node else node['name']
            (node_label, locale) = self.process_node_label(node)
            try:
                locale = settings.LOCALES[locale]
            except Exception as e:
                locale = 'English'
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dict_item = DictionaryItems(
                form_group=self.cur_form_group,
                parent_node=parent_node,
                date_created=now,
                date_modified=now,
                t_key=node['name'],
                t_type=node_type,
                t_locale=locale,
                t_value=node_label
            )
            try:
                dict_item.publish()
            except IntegrityError as e:
                # We can live with this
                terminal.tprint(str(e), 'fail')
            except Exception as e:
                sentry.captureException()
                raise

            if 'mappes' in connections:
                # add the dictionary item to the final database too. It is expecting that the table exists
                insert_q = '''
                    INSERT INTO dictionary_items(form_group, parent_node, t_key, t_type, t_locale, t_value, date_created, date_modified)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                '''
                with connections['mapped'].cursor() as cursor:
                    try:
                        cursor.execute(insert_q, (self.cur_form_group, parent_node, node['name'], node_type, locale, node_label, now, now))
                    except IntegrityError as e:
                        # We can live with this
                        terminal.tprint(str(e), 'fail')
                    except Exception as e:
                        sentry.captureException()
                        raise

            if 'type' in node:
                if node['type'] == 'select one' or node['type'] == 'select all that apply':
                    if 'children' in node:
                        for child in node['children']:
                            self.add_dictionary_items(child, 'choice', node['name'])
        # else:
        #     terminal.tprint("\tThe node %s is already saved, skipping it" % node['name'], 'okblue')

    def process_node_label(self, t_node):
        '''
        Process a label node and returns the proper label of the node
        '''
        node_type = self.determine_type(t_node['label'])
        if node_type == 'is_json':
            try:
                cur_label = t_node['label'][settings.DEFAULT_LOCALE]
                locale = settings.DEFAULT_LOCALE
            except Exception as e:
                terminal.tprint("There was an error (%s) while getting a node label from:\n\t%s" % (str(e), json.dumps(t_node)), 'debug')
                terminal.tprint("I will get the first label", 'warn')
                cur_label = list(t_node['label'].values())[0]
                locale = list(t_node['label'].keys())[0]
        elif node_type == 'is_string':
            cur_label = t_node['label']
            locale = settings.DEFAULT_LOCALE
        else:
            raise Exception('Cannot determine the type of label that I have got! %s' % json.dumps(t_node['label']))

        return cur_label, locale

    def add_to_all_nodes(self, t_node):
        # add a node to the list of all nodes for creating the tree
        if 'items' in t_node:
            del t_node['items']

        if 'label' in t_node:
            (cur_label, locale) = self.process_node_label(t_node)
            t_node['label'] = cur_label
            # terminal.tprint('searching cur label' + cur_label, 'debug')
            if re.search(":$", cur_label) is not None:
                # in case the label was ommitted, use the name tag
                t_node['label'] = t_node['name']

        self.all_nodes.append(t_node)

    def initiate_form_database(self, form_name):
        self.form_connection = Query(form_name)
        self.form_connection.register_database()

        return False

    def delete_folder_contents(self, folder_path):
        """
        Given a path to a folder, delete its contents
        """
        for filename in os.listdir(folder_path):
            if filename == '.' or filename == '..':
                    continue
            terminal.tprint("Deleting '%s'" % folder_path + os.sep + filename, 'fail')
            os.unlink(folder_path + os.sep + filename)

    def save_user_view(self, form_id, view_name, nodes, all_submissions, structure, form_group, repopulate=False):
        """
        Given a view with a section of the user defined data, create a view of the selected nodes
        """
        # get a proper view name
        prop_view_name = self.formulate_view_name(view_name, form_group)

        # save the submissions as an excel an then call a function to create the table(s)
        # create a temp dir for this
        if not os.path.exists(settings.TEMPDIR):
            os.makedirs(settings.TEMPDIR)

        full_path = os.path.join(settings.TEMPDIR, prop_view_name)
        if os.path.exists(full_path):
            self.delete_folder_contents(full_path)
        else:
            # create the directory
            terminal.tprint("Create the directory '%s'" % full_path, 'warn')
            os.makedirs(full_path)

        # writer = ExcelWriter(prop_view_name, 'csv', prop_view_name)
        excel_filename = os.path.join(full_path, '%s.xlsx' % prop_view_name)
        writer = ExcelWriter(excel_filename, 'csv', full_path)
        writer.create_workbook(all_submissions, structure)
        terminal.tprint("\tFinished creating the csv sheets '%s'" % excel_filename, 'warn')

        # now we have all our selected submissions as csv files, so process them
        try:
            table_views = self.save_csvs_to_database(full_path, prop_view_name, repopulate)
        except Exception:
            raise

        # add the tables to the lookup table of views
        try:
            form_view = FormViews.objects.filter(view_name=view_name)
            odk_form = ODKForm.objects.get(form_id=form_id)
            if form_view.count() == 0:
                # save the new view
                form_view = FormViews(
                    form=odk_form,
                    view_name=view_name,
                    proper_view_name=prop_view_name,
                    structure=nodes
                )
                form_view.publish()
            elif form_view.count() > 0 and repopulate:
                # we already have the saved view, need to repopulate the data
                pass
            else:
                # we have a duplicate form view
                raise ValueError("The view '%s' is already saved and we are not repopulating the data!" % view_name)

            form_view = FormViews.objects.get(view_name=view_name)

            # save these submissions to the database
            terminal.tprint("\tSaving the views extracted submissions", 'okblue')
            if repopulate:
                ViewsData.objects.filter(view=form_view).delete()
            for submission in all_submissions:
                new_submission = ViewsData(
                    view=form_view,
                    raw_data=submission
                )
                new_submission.publish()

            # now save the created views
            for view in table_views:
                terminal.tprint("\tSaving the view lookup table '%s'" % view, 'warn')
                existing_view_lookup = ViewTablesLookup.objects.filter(hashed_name=view['hashed_name'])
                if existing_view_lookup.count() == 0:
                    cur_view = ViewTablesLookup(
                        view=form_view,
                        table_name=view['table_name'],
                        hashed_name=view['hashed_name']
                    )
                    cur_view.publish()

            # clean up process
            # delete the generated files
            self.delete_folder_contents(full_path)
            os.rmdir(full_path)
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            sentry.captureException()

            # lets delete the created tables
            terminal.tprint('\tClean up on error', 'okblue')
            with connection.cursor() as cursor:
                for view in table_views:
                    terminal.tprint("\t\tDrop table '%s'" % view['table_name_hash_dig'], 'okblue')
                    dquery = "drop table %s" % view.table_name_hash_dig
                    cursor.execute(dquery)

        # if repopulate and form_view.count() > 0:
            # save the view overwriting what was there previously if
            # asked to repopulate
            # Delete the existing view with the given view_name
            # existing_form_view = FormViews.objects.filter(view_name=view_name)
            # existing_form_view.delete()
            # logger.error(existing_form_view)

            # form_view = FormViews(
            #     form=odk_form,
            #     view_name=view_name,
            #     proper_view_name=prop_view_name,
            #     structure=nodes
            # )
            # form_view.publish()
            #
            # # save these submissions to the database
            # for submission in all_submissions:
            #     new_submission = ViewsData(
            #         view=form_view,
            #         raw_data=submission
            #     )
            #     new_submission.publish()
            # pass
        # else:
        #     logger.error("Duplicate view name '%s'. Can't save." % view_name)
            # raise Exception("Duplicate view name '%s'. Can't save." % view_name)
            # return

    def save_csvs_to_database(self, full_path, prop_view_name, repopulate):
        # import_command = "csvsql --db 'postgresql:///%s?user=%s&password=%s' --encoding utf-8 --blanks --insert --tables %s %s"
        import_command = "env/bin/csvsql --db '%s:///%s?user=%s&password=%s' --encoding utf8 --blanks %s --no-inference --insert --tables %s %s"
        terminal.tprint(import_command, 'warn')
        table_views = []
        for filename in os.listdir(full_path):
            if filename == '.' or filename == '..' or re.match('^_', filename):
                continue

            terminal.tprint("Processing the file '%s'" % filename, 'okblue')
            basename = os.path.splitext(filename)[0]
            table_name = '%s_%s' % (prop_view_name, basename)
            table_name_hash = hashlib.md5(table_name.encode('utf-8'))
            terminal.tprint("\tHashed the table name '%s'" % table_name, 'warn')
            table_name_hash_dig = "v_%s" % table_name_hash.hexdigest()

            # check if the table exists
            try:
                with connection.cursor() as cursor:
                    dquery = "select * from %s" % table_name_hash_dig
                    cursor.execute(dquery)
                is_create_table = '--no-create' if repopulate else ''
            except Exception as e:
                terminal.tprint("\tNeed to create the table '%s'. It doesnt exist. \n\t'%s'" % (table_name_hash_dig, str(e)), 'okblue')
                # we need to create the able, it doesn't exist
                is_create_table = ''

            # print (table_name_hash_dig)
            terminal.tprint("\tHashed the table name '%s' to '%s'" % (table_name, table_name_hash_dig), 'ok')

            filename = os.path.join(full_path, filename).encode('utf-8')
            base_name, file_extension = os.path.splitext(filename)

            terminal.tprint("\tProcessing the file '%s' for saving to the database" % filename, 'warn')
            print (file_extension)
            if file_extension.decode('ascii') == '.csv':
                cmd = import_command % (
                    settings.DATABASES['default']['DRIVER'],
                    settings.DATABASES['default']['NAME'],
                    settings.DATABASES['default']['USER'],
                    settings.DATABASES['default']['PASSWORD'],
                    is_create_table,
                    table_name_hash_dig,
                    filename.decode('utf-8'),
                )

                try:
                    if repopulate and is_create_table != '':
                        # truncate the table inorder to add new data
                        with connection.cursor() as cursor:
                            dquery = "truncate table %s" % table_name_hash_dig
                            cursor.execute(dquery)
                    terminal.tprint("\tRunning the command '%s'" % cmd, 'ok')
                    # run commands to create primary key
                    print((subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()))
                except Exception as e:
                    terminal.tprint("\tError while converting the csv file '%s' to a table '%s'.\n\t%s" % (filename, table_name, str(e)), 'fail')
                    sentry.captureException()
                    raise Exception("\tError while converting the csv file '%s' to a table '%s'." % (filename, table_name))

                try:
                    if repopulate is False or is_create_table == '':
                        self.add_dynamic_table_keys(table_name, table_name_hash_dig, repopulate, is_create_table)
                except Exception:
                    raise
                table_views.append({'table_name': table_name, 'hashed_name': table_name_hash_dig})

        return table_views

    def add_dynamic_table_keys(self, table_name, table_name_hash_dig, repopulate, is_create_table):
        try:
            with connection.cursor() as cursor:
                logging.debug("Adding a primary key constraint for the table '%s'" % table_name)
                query = "alter table %s add primary key (%s)" % (table_name_hash_dig, 'unique_id')
                cursor.execute(query)

                # if table name has a main on it, it must have a _uuid field which should be unique
                if re.search("main$", table_name) is not None:
                    # this is finicky, omit it for now
                    terminal.tprint("Not adding a unique constraint for column '_uuid'", 'fail')
                    # logging.debug("Adding unique constraint '%s' for the table '%s'" % ('_uuid', table_name))
                    # uquery = "alter table %s add constraint %s_%s unique (%s)" % (table_name_hash_dig, table_name_hash_dig, 'uuid', '_uuid')
                    # cursor.execute(uquery)
                else:
                    # for the other tables, add an index to top_id
                    logging.debug("Adding indexes to '%s' and '%s' for the table '%s'" % ('top_id', 'parent_id', table_name))
                    uquery = "create index %s_%s on %s (%s)" % (table_name_hash_dig, 'top_id', table_name_hash_dig, 'top_id')
                    cursor.execute(uquery)
                    uquery = "create index %s_%s on %s (%s)" % (table_name_hash_dig, 'parent_id', table_name_hash_dig, 'parent_id')
                    cursor.execute(uquery)
        except Exception as e:
            logging.error("For some reason can't create a primary key or unique key, raise an error and delete the view")
            terminal.tprint(str(e), 'fail')
            if repopulate is False or is_create_table == '':
                with connection.cursor() as cursor:
                    dquery = "drop table %s" % table_name_hash_dig
                    cursor.execute(dquery)
            sentry.captureException()
            raise Exception("For some reason I can't create a primary key or unique key for the table %s. Deleting it entirely" % table_name)

    def formulate_view_name(self, view_name, form_group):
        """
        Formulate a proper view name that will be used as the view name in the database
        """
        # convert all to lowercase
        view_name = view_name.lower()

        # convert non alpha numeric characters to spaces
        view_name = re.sub(r"[^a-zA-Z0-9]+", '_', view_name)
        form_group = re.sub(r"[^a-zA-Z0-9]+", '_', form_group)

        # create a unique view name
        view_name = "%s_%s" % (form_group, view_name)
        return view_name

    def formulate_db_name(self, form_name):
        # convert all to lowercase
        db_name = form_name.lower()
        db_name = db_name.replace('.', '_')
        return db_name

    def fetch_merge_data(self, form_id, nodes, d_format, download_type, view_name, uuids=None, update_local_data=True, is_dry_run=True, submission_filters=None):
        """
        Given a form id and nodes of interest, get data from all associated forms
        Args:
            form_id (TYPE): Description
            nodes (TYPE): Description
            d_format (TYPE): Description
            download_type (TYPE): Description
            view_name (TYPE): Description
            uuids (None, optional): A list of uuids to use to fetch the corresponding data instead of fetching all the submitted data
            update_local_data (bool, optional): Whether or not to update the local dataset
        Returns:
            TYPE: Description
        Raises:
            Exception: Description
        """

        # print( "%s - %s - %s - %s - %s - %s" % (form_id, nodes, d_format, download_type, view_name, submission_filters))
        view_name = None if view_name == '' else view_name
        associated_forms = []
        try:
            cur_form = ODKForm.objects.get(form_id=form_id)
            if cur_form.form_group_id is None:
                # we have a form not belonging to a group
                associated_forms.append(cur_form.form_id)
                form_name = 'No group defined'
            else:
                form_group = ODKFormGroup.objects.get(id=cur_form.form_group_id)

                # get all the form ids belonging to the same group
                temp_forms = ODKForm.objects.filter(form_group_id=cur_form.form_group_id)
                for t_form in temp_forms:
                    associated_forms.append(t_form.form_id)
                form_name = form_group.group_name
        except Exception as e:
            print((traceback.format_exc()))
            # there is an error getting the associated forms, so get data from just one form
            terminal.tprint(str(e), 'fail')
            associated_forms.append(form_id)
            form_name = "Form%s" % str(form_id)
            sentry.captureException()
            logging.info(str(e))

        # having all the associated form ids, fetch the required data
        all_submissions = []

        # since we shall be merging similar forms as one, declare the indexes here
        self.cur_node_id = 0
        self.indexes = {}
        self.sections_of_interest = {}
        self.output_structure = {'main': ['unique_id']}
        self.indexes['main'] = 1

        for form_id in associated_forms:
            try:
                if submission_filters is not None:
                    if len(submission_filters) == 0:
                        submission_filters = None
                this_submissions = self.get_form_submissions_as_json(int(form_id), nodes, uuids, update_local_data, is_dry_run, submission_filters)
            except Exception as e:
                # logging.debug(traceback.format_exc())
                # logging.error(str(e))
                print((traceback.format_exc()))
                terminal.tprint(str(e), 'fail')
                sentry.captureException()
                # raise Exception(str(e))
                continue

            if this_submissions is None:
                continue
            else:
                # terminal.tprint("\tCurrent no of submissions %d" % len(this_submissions), 'warn')
                all_submissions = copy.deepcopy(all_submissions) + copy.deepcopy(this_submissions)

        # terminal.tprint("\tTotal no of submissions %d" % len(all_submissions), 'ok')
        if len(all_submissions) == 0:
            terminal.tprint("The form (%s) has no submissions for download" % str(form_name), 'fail')
            logging.debug("The form (%s) has no submissions for download" % str(form_name))
            if download_type == 'download_save':
                return {'is_downloadable': False, 'error': False, 'message': "The form (%s) has no submissions for download" % str(form_name)}
            elif download_type == 'submissions':
                return all_submissions

        # check if there is need to create a database view of this data
        if download_type == 'download_save' or update_local_data:
            try:
                # save the view if we have a view_name
                if view_name is not None:
                    self.save_user_view(form_id, view_name, nodes, all_submissions, self.output_structure, form_group.group_name, update_local_data)
            except Exception as e:
                sentry.captureException()
                return {'is_downloadable': False, 'error': True, 'message': str(e)}
        elif download_type == 'submissions':
            return all_submissions

        # now we have all the submissions, create the Excel sheet
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        if d_format == 'xlsx':
            # now lets save the data to an excel file
            output_name = './' + form_name + '_' + now + '.xlsx'
            self.save_submissions_as_excel(all_submissions, self.output_structure, output_name)
            return {'is_downloadable': True, 'filename': output_name}
        else:
            return all_submissions

    def save_submissions_as_excel(self, submissions, structure, filename):
        writer = ExcelWriter(filename)
        writer.create_workbook(submissions, structure)

    def get_form_submissions_as_json(self, form_id, screen_nodes, uuids=None, update_local_data=True, is_dry_run=True, submission_filters=None):
        """Given a form id get the form submissions
        
        If the screen_nodes is given, process and return only the subset of data in those forms
        If the uuids are given, fetch only the datasets with the given uuids
        
        Args:
            form_id (TYPE): Description
            screen_nodes (TYPE): Description
            uuids (None, optional): Description
            update_local_data (bool, optional): Whether or not to update the local dataset
        
        Returns:
            TYPE: Description
        """
        # given a form id get the form submissions
        # 

        submissions_list = self.get_all_submissions(form_id, uuids, update_local_data)

        if submissions_list is None or submissions_list.count() == 0:
            terminal.tprint("The form with id '%s' has no submissions returning as such" % str(form_id), 'fail')
            return None

        try:
            # get the form metadata
            # cur_form = ODKForm.objects.get(form_id=form_id)
            # form_group = ODKFormGroup.objects.get(id=cur_form.form_group_id)

            # get the fields to include as part of the form metadata
            # todo: Add option of specifying metadata
            form_meta = ['_uuid', 's1p1q3_country', 's1p1q3_sel_country']
            self.pk_name = 'hh_'
            self.sk_format = 'id_'
        except Exception as e:
            terminal.tprint("Form settings for form id (%d) haven't been defined" % form_id, 'fail')
            logger.info("The settings for the form id (%s) haven't been defined" % str(form_id))
            logger.debug(e)
            form_meta = []
            self.pk_name = 'hh_id'

        if screen_nodes is not None:
            screen_nodes.extend(form_meta)
            screen_nodes.append('unique_id')

            # ensure the nodes are unique
            screen_nodes = list(set(screen_nodes))

        submissions = []
        if submission_filters is not None:
            # create a dictionary which will contain the details of the filters, ie, the short_field_name, full_field_name and the filter criteria
            # terminal.tprint("\tWe are going to filter the data using the criteria: "+ json.dumps(submission_filters), 'debug')
            self.filters_in_detail = {}
        if is_dry_run:
            i = 0
        for data in submissions_list:
            if is_dry_run:
                i = i + 1
                if i > settings.DRY_RUN_RECORDS:
                    terminal.tprint("\tWe have processed the maximum number of submissions under dry ran settings", 'okblue')
                    break
            # data, csv_files = self.post_data_processing(data)
            pk_key = self.pk_name + str(self.indexes['main'])
            # terminal.tprint(json.dumps(data), 'okblue')
            # terminal.tprint(json.dumps(data), 'warn')

            if self.determine_type(data) == 'is_json' and 'raw_data' in data:
                # terminal.tprint('Is postgres db', 'okblue')
                # terminal.tprint(json.dumps(data), 'okblue')
                data = json.loads(data['raw_data'])
                # terminal.tprint("\t%s" % data, 'ok')
                # terminal.tprint("\tType before conversion: %s" % self.determine_type(data), 'warn')
                if (self.determine_type(data) == 'is_string'):
                    # m = re.findall(r"^'(.+)'$", data)
                    # terminal.tprint(json.dumps(m), 'okblue')
                    try:
                        data = json.loads(data.strip('\'').replace('\\\\"', ''))

                        if self.determine_type(data) == 'is_string':
                            terminal.tprint("\tGiving up. I can't convert the string data to a json object.", 'fail')
                            print(data)
                            raise ValueError("Giving up. I can't convert the string data to a json object.")

                        # terminal.tprint("\tAfter conversion: %s" % self.determine_type(data), 'okblue')
                    except Exception as e:
                        terminal.tprint("\t%s" % str(e), 'fail')
                        terminal.tprint("\t%s" % json.dumps(data), 'fail')
                        raise
            else:
                # terminal.tprint('Is MySQL db', 'okblue')
                data = json.loads(data)

            data['unique_id'] = pk_key
            if submission_filters is not None:
                # try opportunistic checking if the filter keys are in the top level of the dictionary
                if not self.determine_submission_filtering(data, submission_filters):
                    # though shall not pass, you have failed the filter criteria
                    continue

            data = self.process_node(data, 'main', screen_nodes, True)

            submissions.append(data)
            self.indexes['main'] += 1

        return submissions

    def determine_submission_filtering(self, data, filters):
        # given the data and the filter criteria, determine if this submission should be included in the final dataset
        # we assume all data shoud pass, until it does not satisfy one filter
        for f_key, f_data in list(filters.items()):
            # check if we have already processed the filter before
            if f_key not in self.filters_in_detail:
                self.filters_in_detail[f_key] = {'value': f_data}
                for key, value in list(data.items()):
                    # should we match tied to the end???
                    if re.search(re.escape(f_key) + r"$", key):
                        # we have a field which we need to use for filtering
                        terminal.tprint("\tFound the field (%s) corresponding to the filter criteria (%s)" % (key, f_key), 'debug')
                        self.filters_in_detail[f_key]['full_field_name'] = key

            if 'full_field_name' not in self.filters_in_detail[f_key]:
                # we dont have the filter field in this dataset, so we just continue (odd though since the filter field ought to be mandatory in all submissions)
                continue

            # we now have our field and the value, check if the current data should pass based on the value. the filter values are an array of items in lower case
            # terminal.tprint("\tShould '%s' pass when compared to '%s?'" % (data[self.filters_in_detail[f_key]['full_field_name']].lower(), ''.join(['{0}{1}'.format(k, v) for k, v in self.filters_in_detail[f_key]['value'].iteritems()])), 'debug')
            # terminal.tprint("\t%s -- %s" %(self.filters_in_detail[f_key]['full_field_name'], self.filters_in_detail[f_key]['value']), 'debug')
            if data[self.filters_in_detail[f_key]['full_field_name']].lower() in self.filters_in_detail[f_key]['value']:
                return True

        # if we are here, it means all the filters if any were satisfied, so data should pass
        return False

    def process_node(self, node, sheet_name, nodes_of_interest=None, add_top_id=True):
        # the sheet_name is the name of the sheet where the current data will be saved
        cur_node = {}

        for key, value in six.iteritems(node):
            # clean the key
            clean_key = self.clean_json_key(key)
            if clean_key == '_geolocation':
                continue

            val_type = self.determine_type(value)

            # terminal.tprint("%s ==> %s" % (key, clean_key), 'okblue')
            if nodes_of_interest is not None:
                if clean_key not in nodes_of_interest:
                    # if we have a list or json as the value_type, allow further processing
                    if val_type != 'is_list' and val_type != 'is_json':
                        # logger.warn('%s is not in the nodes of interest -- %s' % (clean_key, json.dumps(nodes_of_interest)))
                        continue

            # Check whether there is need to clean the data. If there is need, clean it automatically
            if clean_key in self.cleaners:
                value = self.get_clean_data_value(self.cleaners[clean_key], value)

            is_json = True

            if val_type == 'is_list':
                # temporarily add the current clean_key to the nodes of interest, to see if there is hidden data in the current node
                if nodes_of_interest is not None and clean_key not in nodes_of_interest:
                    # terminal.tprint('\tTemporarily adding %s to the nodes of interest' % clean_key, 'okblue')
                    nodes_of_interest.append(clean_key)
                value = self.process_list(value, clean_key, node['unique_id'], nodes_of_interest, add_top_id)

                if len(self.output_structure[clean_key]) == 3:
                    # there is nothing to save from this sheet, so remove it from the structure
                    del self.output_structure[clean_key]
                    if nodes_of_interest is not None:
                        nodes_of_interest.remove(clean_key)
                    continue

                is_json = False
            elif val_type == 'is_json':
                is_json = True
            elif val_type == 'is_zero':
                is_json = False
                value = 0
            elif val_type == 'is_none':
                terminal.tprint(key, 'warn')
                # print(value)
                is_json = False
                value = 'N/A'
            else:
                is_json = False

            if is_json is True:
                node_value = self.process_node(value, clean_key, nodes_of_interest, add_top_id)
                cur_node[clean_key] = node_value

                # add this key to the sheet name
                if clean_key not in self.output_structure[sheet_name]:
                    self.output_structure[sheet_name].append(clean_key)
            else:
                node_value = value
                cur_node[clean_key] = value

                # add this key to the sheet name
                if clean_key not in self.output_structure[sheet_name]:
                    self.output_structure[sheet_name].append(clean_key)

            """
            if nodes_of_interest is not None:
                # at this point, we have our data, no need to check if we have the right key
                terminal.tprint("\tAdding the processed node (%s)" % clean_key, 'ok')
                if clean_key not in self.sections_of_interest:
                    self.sections_of_interest[clean_key] = []

                if isinstance(node_value, list):
                    for node_item in node_value:
                        self.sections_of_interest[clean_key].append(node_item)
                else:
                    self.sections_of_interest[clean_key].append(node_value)
            """
            if len(cur_node) != 0:
                # logger.info('%s: Found something in the current node' % clean_key)
                if add_top_id is True:
                    cur_node['top_id'] = self.pk_name + str(self.indexes['main'])

        return cur_node

    def determine_type(self, input):
        """
        determine the input from the user

        @todo, rely on the xls form to get the input type
        """
        try:
            float(input) + 2
        except Exception:
            if isinstance(input, list) is True:
                return 'is_list'
            elif input is None:
                return 'is_none'
            elif isinstance(input, dict) is True:
                return 'is_json'
            elif input == '0E-10':
                return 'is_zero'
            else:
                try:
                    json.loads(input)
                except ValueError:
                    if isinstance(input, six.string_types) is True:
                        return 'is_string'

                    terminal.tprint(str(input), 'fail')
                    return 'is_none'
                except Exception:
                    # try encoding the input as string
                    try:
                        json.loads(str(input))
                    except ValueError:
                        return 'is_json'
                    except Exception:
                        terminal.tprint(json.dumps(input), 'fail')
                        return 'is_none'
                    return 'is_json'
                return 'is_json'

        return 'is_int'

    def process_list(self, this_list, sheet_name, parent_key, nodes_of_interest, add_top_id):
        # at times the input is a string and not necessary a json object

        # the sheet name is where to put this subset of data
        if sheet_name not in self.output_structure:
            self.output_structure[sheet_name] = ['unique_id', 'top_id', 'parent_id']
            self.indexes[sheet_name] = 1

        cur_list = []
        for node in this_list:
            val_type = self.determine_type(node)
            node['unique_id'] = sheet_name + '_' + str(self.indexes[sheet_name])

            if val_type == 'is_json':
                processed_node = self.process_node(node, sheet_name, nodes_of_interest, add_top_id)
                processed_node['parent_id'] = parent_key
                cur_list.append(processed_node)
            elif val_type == 'is_list':
                processed_node = self.process_list(node, sheet_name, nodes_of_interest, add_top_id)
                cur_list.append(processed_node)
            else:
                cur_list.append(node)

            self.indexes[sheet_name] += 1

        return cur_list

    def post_data_processing(self, data, csv_files):
        new_data = {}
        for key, node in six.iteritems(data):
            if isinstance(node, list) is True:
                if key not in csv_files:
                    csv_files[key] = []

        return (new_data, csv_files)

    def clean_json_key(self, j_key):
        # given a key from ona with data, get the sane(last) part of the key
        m = re.findall("/?([\.\w]+)$", j_key)
        return m[0]

    def get_clean_data_value(self, cleaners, code):
        try:
            for c_code, country in six.iteritems(cleaners):
                if re.search(c_code, code, re.IGNORECASE) is not None:
                    return country

            # Found nothing, return original code
            return code
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            sentry.captureException()
            return code

    def process_curl_request(self, url):
        """
        Create and execute a curl request
        """
        headers = {'Authorization': "Token %s" % self.ona_api_token}
        # terminal.tprint("\Token %s" % self.ona_api_token, 'ok')
        terminal.tprint("\tProcessing API request %s" % url, 'okblue')
        try:
            r = requests.get(url, headers=headers)
        except ConnectionError as e:
            raise ConnectionError('There was an error while connecting to the ONA server. %s' % str(e))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            sentry.captureException()
            return None

        if r.status_code == 200:
            # terminal.tprint("\tResponse %d" % r.status_code, 'ok')
            # terminal.tprint(json.dumps(r.json()), 'warn')
            return r.json()
        else:
            terminal.tprint("\tResponse %d" % r.status_code, 'fail')
            terminal.tprint(r.text, 'fail')
            terminal.tprint(url, 'warn')

            return None

    def get_views_info(self):
        form_views = FormViews.objects.all()

        all_data = {'views': []}
        for form_view in form_views:
            views_sub_table = ViewTablesLookup.objects.filter(view_id=form_view.id)
            view_date = form_view.date_created.strftime("%Y-%m-%d")
            all_data['views'].append({
                'view_id': form_view.id,
                'view_name': form_view.view_name,
                'date_created': view_date,
                'no_sub_tables': views_sub_table.count(),
                'auto_process': 'Yes',
                'actions': '<button type="button" data-identifier="%s" class="refresh_view_data btn btn-sm btn-outline btn-warning">Refresh Data</button>' % form_view.id
            })
        return all_data

    def delete_view(self, request):
        view = json.loads(request.POST['view'])
        view_id = int(view['view_id'])
        try:
            # first delete the records in the views_table
            view_tables = ViewTablesLookup.objects.filter(view_id=view_id)
            for fview in view_tables:
                # delete the table
                logging.error("Drop the table '%s' in the view '%s'" % (fview.hashed_name, view['view_id']))
                with connection.cursor() as cursor:
                    # delete the actual view itself
                    dquery = "drop table %s" % fview.hashed_name
                    cursor.execute(dquery)
                # now delete the record
                fview.delete()

            # delete the view record in the database
            ViewsData.objects.filter(view_id=view_id).delete()
            FormViews.objects.filter(id=view_id).delete()
            return {'error': False, 'message': 'View deleted successfully'}
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            sentry.captureException()
            return {'error': True, 'message': str(e)}

    def edit_view(self, request):
        try:
            view = json.loads(request.POST['view'])
            # delete the actual view itself
            form_view = FormViews.objects.get(id=view['view_id'])
            form_view.view_name = view['view_name']
            # form_view.auto_process = view['auto_process']
            form_view.publish()

            return {'error': False, 'message': 'View edited successfully'}
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            sentry.captureException()
            return {'error': True, 'message': str(e)}

    def proccess_submissions_count(self, s_count, use_zero=False):
        terminal.tprint(json.dumps(s_count), 'warn')

        to_return = 0
        if len(s_count) == 0:
            to_return = 0.001
        elif s_count[0][0] is None:
            to_return = 0.001
        else:
            to_return = int(s_count[0][0]) if len(s_count) != 0 else 0.001

        if to_return == 0.001:
            if use_zero is True:
                to_return = 0

        return to_return

    def save_mapping(self, request):
        '''
        @todo Add data validation
        '''
        try:
            data = json.loads(request.body)

            cur_form = ODKForm.objects.get(form_id=data['form']['id'])
            form_group = ODKFormGroup.objects.get(id=cur_form.form_group_id)

            # Check if it a foreign key
            is_foreign_key = self.foreign_key_check(settings.DATABASES['mapped']['NAME'], data['table']['title'], data['drop_item']['title'])
            if is_foreign_key is not False and is_foreign_key[0] is not None:
                ref_table_name = is_foreign_key[1]
                ref_column_name = is_foreign_key[2]
            else:
                ref_table_name = None
                ref_column_name = None

            mapping = FormMappings(
                form_group=form_group.group_name,
                form_question=data['table_item']['name'],
                dest_table_name=data['table']['title'],
                dest_column_name=data['drop_item']['title'],
                odk_question_type=data['table_item']['type'],
                db_question_type=data['drop_item']['type'],
                ref_table_name=ref_table_name,
                ref_column_name=ref_column_name
            )
            mapping.publish()
        except Exception as e:
            terminal.tprint(str(e), 'ok')
            sentry.captureException()
            return {'error': True, 'message': str(e)}

        mappings = self.mapping_info()
        return {'error': False, 'mappings': mappings}

    def edit_mapping(self, request):
        try:
            mapping = json.loads(request.POST['mapping'])
            # delete the actual view itself
            cur_mapping = FormMappings.objects.get(id=mapping['mapping_id'])
            if mapping['regex_validator'] is not None and mapping['regex_validator'] != '':
                try:
                    re.compile(mapping['regex_validator'])
                except re.error:
                    sentry.captureException()
                    return {'error': True, 'message': 'The specified REGEX is not valid!'}

                cur_mapping.validation_regex = mapping['regex_validator']

            if mapping['is_record_id'] is not None:
                cur_mapping.is_record_identifier = mapping['is_record_id']

            if mapping['is_lookup_id'] is not None:
                cur_mapping.is_lookup_field = mapping['is_lookup_id']

            cur_mapping.publish()
            mappings = self.mapping_info()

            return {'error': False, 'message': 'The mapping was updated successfully', 'mappings': mappings}
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(str(e))
            sentry.captureException()
            return {'error': True, 'message': str(e)}

    def mapping_info(self):
        all_mappings = FormMappings.objects.select_related().all().order_by('dest_table_name').order_by('dest_column_name')

        to_return = []
        for mapping in all_mappings:
            cur_mapping = model_to_dict(mapping)
            cur_mapping['_checkbox'] = '<input type="checkbox" class="row-checkbox">'
            cur_mapping['mapping_id'] = cur_mapping['id']
            to_return.append(cur_mapping)
        return to_return

    def clear_mappings(self):
        FormMappings.objects.all().delete()

        mappings = self.mapping_info()
        return {'error': False, 'mappings': mappings}

    def delete_mapping(self, request):
        data = json.loads(request.POST['mappings'])
        FormMappings.objects.filter(id=data['mapping_id']).delete()

        mappings = self.mapping_info()
        return {'error': False, 'mappings': mappings}

    def get_db_tables(self):
        with connections['mapped'].cursor() as cursor:
            tables_q = "SHOW tables"
            cursor.execute(tables_q)
            tables = cursor.fetchall()

            all_tables = []
            all_tables_columns = []
            all_tables.append({'title': 'Select One Table', 'id': '-1'})
            for parent_index, table in enumerate(tables):
                all_tables.append({'title': table[0], 'id': parent_index})
                columns_q = 'DESC %s' % table
                cursor.execute(columns_q)
                all_columns = cursor.fetchall()
                for index, col in enumerate(all_columns):
                    all_tables_columns.append({'title': col[0], 'type': col[1], 'id': index + 1000, 'parent_id': parent_index, 'label': '%s (%s)' % (col[0], col[1])})

        return all_tables, all_tables_columns

    def validate_mappings(self):
        '''
        Validate the mappings and ensure all mandatory fields have been mapped
        '''
        # get all the mapped tables
        form_groups = list(ODKFormGroup.objects.all().order_by('order_index'))

        is_fully_mapped = True
        is_mapping_valid = True
        comments = []
        for form_group in form_groups:
            terminal.tprint("Beginning form group '%s' validation" % form_group.group_name, 'okblue')
            mapped_tables = list(FormMappings.objects.filter(form_group=form_group.group_name).values('dest_table_name').distinct())
            self.validated_tables = []
            self.tables_being_validated = []

            for table in mapped_tables:
                (is_table_fully_mapped, is_table_mapping_valid, table_comments) = self.validate_mapped_table(table['dest_table_name'])
                is_fully_mapped = is_fully_mapped and is_table_fully_mapped
                is_mapping_valid = is_mapping_valid and is_table_mapping_valid
                comments.extend(table_comments)

        return is_fully_mapped, is_mapping_valid, comments

    def validate_mapped_table(self, table):
        terminal.tprint('\tValidating mapped table - "%s"' % table, 'warn')
        self.tables_being_validated.append(table)
        comments = []
        is_fully_mapped = True
        is_mapping_valid = True
        mapped_columns = FormMappings.objects.filter(dest_table_name=table)
        all_mapped_columns = {}
        has_primary_key = False
        for col in mapped_columns:
            all_mapped_columns[col.dest_column_name] = model_to_dict(col)

        # terminal.tprint(json.dumps(all_mapped_columns), 'fail')
        with connections['mapped'].cursor() as mapped_cursor:
            dest_columns_q = 'DESC %s' % table
            mapped_cursor.execute(dest_columns_q)
            dest_columns = mapped_cursor.fetchall()

            # loop through all the destination columns and ensure mandatory fields have been mapped
            for dest_column in dest_columns:
                # if we have a lookup table, note this and continue
                # terminal.tprint(json.dumps(dest_column), 'warn')
                if dest_column[0] in all_mapped_columns:
                    if all_mapped_columns[dest_column[0]]['is_lookup_field'] is True:
                        terminal.tprint("\tWe have found a lookup field '%s'" % dest_column[0], 'warn')
                        continue

                # determine if we have a foreign key
                is_foreign_key = self.foreign_key_check(settings.DATABASES['mapped']['NAME'], table, dest_column[0])
                # If the column is of type int, check if it a foreign key

                if is_foreign_key is not False:
                    if is_foreign_key[0] is not None:
                        if is_foreign_key[1] in self.validated_tables:
                            # We have a FK column whose table is already validated so lets continue
                            continue
                        if table != is_foreign_key[1] and is_foreign_key[1] not in self.tables_being_validated and is_foreign_key[1] != settings.LOOKUP_TABLE:
                            terminal.tprint('\tFound a linked table to validate. Current table: %s, table to validate %s' % (table, is_foreign_key[1]), 'warn')
                            # check that the corresponding table is fully mapped
                            (is_table_fully_mapped, is_table_mapping_valid, table_comments) = self.validate_mapped_table(is_foreign_key[1])
                            if is_table_fully_mapped is False and dest_column[2] == 'NO':
                                mssg = "REFERENTIAL INTEGRITY FAIL: The referenced table '%s' is not fully mapped." % is_foreign_key[1]
                                terminal.tprint('\t%s' % mssg, 'fail')
                                comments.append({'type': 'danger', 'message': mssg})
                                is_fully_mapped = False
                            else:
                                mssg = "We wont process the linked table '%s', it is not fully mapped but the column '%s.%s' is not a mandatory field" % (is_foreign_key[1], table, dest_column[0])
                                comments.append({'type': 'warning', 'message': mssg})
                                terminal.tprint('\t%s' % mssg, 'warn')

                            if not is_table_mapping_valid:
                                mssg = "REFERENTIAL INTEGRITY FAIL: The referenced table '%s' mapping is not valid." % is_foreign_key[1]
                                terminal.tprint('\t%s' % mssg, 'fail')
                                comments.append({'type': 'danger', 'message': mssg})
                                is_mapping_valid = False
                            continue

                # check if the column in mandatory and is included in the mapping
                if dest_column[2] == 'NO':
                    # check if it is a primary key
                    if dest_column[3] == 'PRI':
                        has_primary_key = True
                        if dest_column[5] == 'auto_increment':
                            # its a primary key and auto incrementing, so skip it
                            continue
                    if dest_column[0] not in all_mapped_columns:
                        # check if we have a default value
                        if dest_column[4] is not None:
                            # we have a default value, so if it isn't mapped, we can safely ignore it
                            comments.append({'type': 'warning', 'message': "The column '%s' in the table '%s' is required but it is not mapped, I will use the defined default value." % (dest_column[0], table)})
                            continue
                        comments.append({'type': 'danger', 'message': "The column '%s' in the table '%s' requires a value but it is not mapped" % (dest_column[0], table)})
                        is_fully_mapped = False
                        continue
                else:
                    if dest_column[0] in all_mapped_columns:
                        # the destination column is not mandatory, ensure that it is captured well in the is_null column
                        if all_mapped_columns[dest_column[0]]['is_null'] is None:
                            terminal.tprint("\tThe column '%s' accepts NULL values, but this isn't well captured! Updating the database..." % dest_column[0], 'fail')
                            cur_col = FormMappings.objects.get(id=all_mapped_columns[dest_column[0]]['id'])
                            cur_col.is_null = 1
                            cur_col.publish()

                # check the column data type
                # check the validation regex
                if dest_column[0] in all_mapped_columns:
                    if all_mapped_columns[dest_column[0]]['validation_regex'] is None:
                        comments.append({'type': 'warning', 'message': "Consider adding a validation regex for column '%s' of the table '%s'" % (dest_column[0], table)})

            if has_primary_key is False:
                comments.append({'type': 'danger', 'message': "The referenced table '%s' doesn't have a primary key defined. I wont be able to process the data." % is_foreign_key[1]})
                is_mapping_valid = False

        self.validated_tables.append(table)
        return is_fully_mapped, is_mapping_valid, comments

    def foreign_key_check(self, schema, table, column):
        foreign_key_check_q = '''
            SELECT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' and COLUMN_NAME = '%s'
        ''' % (schema, table, column)

        with connections['mapped'].cursor() as mapped_cursor:
            mapped_cursor.execute(foreign_key_check_q)
            foreign_keys = mapped_cursor.fetchall()

            if len(foreign_keys) == 0:
                return False
            else:
                # we assume that the column is only mapped to only 1 other column
                return foreign_keys[0]

    def populateDestinationTables(self):
        # get all the destination tables from the destination schema and add them to the destination_tables table
        with connections['mapped'].cursor() as cursor:
            tables_q = "SHOW tables"
            cursor.execute(tables_q)
            tables = cursor.fetchall()

            for table in tables:
                dest_table = FormMappings(table_name=table)
                dest_table.publish()

    def manual_process_data(self, is_dry_run, submissions=None):
        # initiate the process of manually processing the data
        # 1. Get the form groups involved in the mapping
        # 2. For each form group, get all the tables which have been mapped
        # 3. Get the raw submissions and try and save the data to the destination tables
        form_groups = list(ODKFormGroup.objects.all().order_by('order_index'))
        top_error = False
        all_comments = []

        for form_group in form_groups:
            self.cur_group_queries = collections.OrderedDict()
            self.all_foreign_keys = defaultdict(dict)
            tables = list(FormMappings.objects.filter(form_group=form_group.group_name).values('dest_table_name').distinct())
            for table in tables:
                try:
                    terminal.tprint("\n\n\nGenerating queries for the group '%s'" % form_group.group_name, 'debug')
                    self.generate_table_query(form_group, table['dest_table_name'], None, None)
                except Exception as e:
                    terminal.tprint('\t%s' % str(e), 'fail')
                    top_error = top_error or True
                    all_comments.append(str(e))
                    break

            # terminal.tprint('\t%s' % json.dumps(self.cur_group_queries), 'warn')
            try:
                (is_error, comments) = self.process_form_group_data(form_group, is_dry_run, submissions)
                all_comments = copy.deepcopy(all_comments) + copy.deepcopy(comments)
                top_error = top_error or is_error
            except Exception as e:
                terminal.tprint('\t%s' % str(e), 'fail')
                top_error = top_error or True
                all_comments.append(str(e))

        return top_error, all_comments

    def generate_table_query(self, form_group, table, ref_table, ref_column):
        """
        Given a form group, process the defined mappings and collect/interpret the mapping definitions
        """
        terminal.tprint("\tGenerating queries for the table '%s'" % table, 'okblue')
        if table in self.cur_group_queries:
            terminal.tprint("\t\tThe query for the table %s has already been generated, skipping it..." % table, 'okblue')
            return False

        terminal.tprint("\t\tGenerating %s's (%s group) query" % (table, form_group.group_name), 'warn')
        # get the table primary key
        # terminal.tprint("\t\tCheck whether table '%s' has a primary key" % table, 'debug')
        primary_key = self.get_table_primary_key(table)
        if primary_key is None:
            mssg = "Invalid Destination Table: The table '%s' does not have a defined primary key. I can't continue" % table
            terminal.tprint(mssg, 'error')
            raise Exception(mssg)

        insert_query = "INSERT INTO %s" % table
        column_names = []
        column_values = []
        duplicate_constraints = ''
        mappings = list(FormMappings.objects.filter(form_group=form_group.group_name).filter(dest_table_name=table).values())
        mapped_columns = []
        dup_columns_sources = []
        source_datapoints = []
        actual_data_nodes = {}             # Source data nodes that are explicitly saved in the db

        cur_table_group = defaultdict(dict)
        cur_table_group['columns'] = defaultdict(dict)
        cur_table_group['dup_check'] = defaultdict(dict)

        for mapping in mappings:
            terminal.tprint("\t\t\tProcessing the mapping: '%s' - '%s' - '%s' - '%s' - '%s'" % (mapping['form_group'], mapping['form_question'], mapping['odk_question_type'], mapping['dest_table_name'], mapping['dest_column_name']), 'warn')
            if mapping['dest_column_name'] in mapped_columns:
                source_datapoints.append(mapping['form_question'])

                if 'is_lookup_field' in cur_table_group['columns'][mapping['dest_column_name']]:
                    # We have another mapping which corresponds to a lookup table, so add it to the list of dict infos
                    # Copy-Pasted code from below! Bad
                    cur_dict = defaultdict(dict)
                    cur_dict['odk_node'] = mapping['form_question']
                    cur_dict['question_type'] = mapping['odk_question_type']
                    cur_dict['form_group'] = form_group.group_name
                    cur_dict['unique_cols'] = []

                    cur_table_group['columns'][mapping['dest_column_name']]['is_lookup_field'] = True

                    # if we have a lookup thing which is not a select, we are expecting some data, so include this in the query
                    cur_dict['is_a_select'] = False if mapping['odk_question_type'] not in ('select one', 'select multiple') else True

                    cur_table_group['columns'][mapping['dest_column_name']]['dict_info'].append(cur_dict)
                else:
                    if mapping['ref_table_name'] is None:
                        # add to the list of actual data points
                        actual_data_nodes[mapping['form_question']] = mapping['dest_column_name']

                    # terminal.tprint(json.dumps(cur_table_group), 'ok')
                    terminal.tprint("\t\t\tFound an instance(%s-%s) where multiple data sources are saved in one table field(%s)'" % (json.dumps(cur_table_group['columns'][mapping['dest_column_name']]['sources']), mapping['form_question'], mapping['dest_column_name']), 'okblue')
                    # we have a scenario where 2 data sources are being saved in one table field
                    cur_table_group['columns'][mapping['dest_column_name']]['has_multiple_sources'] = True
                    cur_table_group['columns'][mapping['dest_column_name']]['sources'].append(mapping['form_question'])

                continue

            if mapping['is_lookup_field'] == 1:
                # we have a lookup field whose data should come from the lookup table. This will be handled later on... continue for now
                terminal.tprint("\t\t\tFound a lookup field '%s' for '%s:%s'" % (mapping['form_question'], mapping['dest_table_name'], mapping['dest_column_name']), 'okblue')

                # Add it to the list of destination columns
                mapped_columns.append(mapping['dest_column_name'])
                source_datapoints.append(mapping['form_question'])

                # add the columns for the FK of attribute id
                column_names.append(mapping['dest_column_name'])
                column_values.append('%s')

                cur_dict = defaultdict(dict)
                if 'dict_info' not in cur_table_group['columns'][mapping['dest_column_name']]:
                    cur_table_group['columns'][mapping['dest_column_name']]['dict_info'] = []
                cur_dict['odk_node'] = mapping['form_question']
                cur_dict['question_type'] = mapping['odk_question_type']
                cur_dict['form_group'] = form_group.group_name
                cur_dict['unique_cols'] = []

                cur_table_group['columns'][mapping['dest_column_name']]['is_lookup_field'] = True

                # if we have a lookup thing which is not a select, we are expecting some data, so include this in the query
                cur_dict['is_a_select'] = False if mapping['odk_question_type'] not in ('select one', 'select multiple') else True

                cur_table_group['columns'][mapping['dest_column_name']]['dict_info'].append(cur_dict)
                continue

            if mapping['odk_question_type'] == 'geopoint':
                cur_table_group['columns'][mapping['dest_column_name']]['is_geopoint'] = True

            if mapping['is_null'] is not None:
                cur_table_group['columns'][mapping['dest_column_name']]['is_nullable'] = True if mapping['is_null'] == 1 else False

            if mapping['ref_table_name'] is not None:
                cur_table_group['columns'][mapping['dest_column_name']]['is_foreign_key'] = True
                cur_table_group['columns'][mapping['dest_column_name']]['ref_table_name'] = mapping['ref_table_name']
                cur_table_group['columns'][mapping['dest_column_name']]['ref_column_name'] = mapping['ref_column_name']
            else:
                # add to the list of actual data points
                actual_data_nodes[mapping['form_question']] = mapping['dest_column_name']

            if mapping['validation_regex'] is not None:
                cur_table_group['columns'][mapping['dest_column_name']]['regex'] = mapping['validation_regex']

            if mapping['is_record_identifier']:
                dc = "%s=%%s" % mapping['dest_column_name']
                duplicate_constraints = dc if duplicate_constraints == '' else '%s and %s' % (duplicate_constraints, dc)
                dup_columns_sources.append(mapping['dest_column_name'])

            if mapping['ref_table_name'] is not None and table != mapping['dest_table_name'] and mapping['is_lookup_field'] != 1:
                # this table should be populated before the current table
                terminal.tprint('\t\t\tThe table "%s" should be populated before "%s"' % (mapping['dest_table_name'], table), 'ok')
                try:
                    self.generate_table_query(form_group, mapping['dest_table_name'], table, mapping['dest_column_name'])
                except Exception:
                    raise

            # lineup to be added to the database
            column_names.append(mapping['dest_column_name'])
            column_values.append("%s")

            mapped_columns.append(mapping['dest_column_name'])
            source_datapoints.append(mapping['form_question'])
            cur_table_group['columns'][mapping['dest_column_name']]['sources'] = [mapping['form_question']]

        # get all foreign keys on this table
        all_fks = self.get_all_foreign_keys(table)
        fks_to_add = {}
        # terminal.tprint(json.dumps(all_fks), 'ok')
        if len(all_fks) != 0:
            self.all_foreign_keys[table] = all_fks
            for cur_fk in all_fks:
                terminal.tprint('\tLinked column %s:%s to %s:%s' % (table, cur_fk['col'], cur_fk['fk_table'], cur_fk['ref_col']), 'ok')
                add_to_query = False
                if cur_fk['fk_table'] in self.cur_group_queries:
                    terminal.tprint('\tWe have found a linked table "%s" which needs to be added to the query' % cur_fk['fk_table'], 'okblue')
                    add_to_query = True
                else:
                    # check if the current linked table is involved in the mapping, if it is, populate it before the current table and add the columns to the current query
                    tables = list(FormMappings.objects.filter(form_group=form_group.group_name).filter(dest_table_name=cur_fk['fk_table']).values('dest_table_name').distinct())

                    if len(tables) != 0:
                        terminal.tprint('Gotcha! The table "%s" should be populated before "%s"' % (cur_fk['fk_table'], table), 'warn')
                        try:
                            self.generate_table_query(form_group, cur_fk['fk_table'], table, cur_fk['col'])
                            # due to the multi level linkages, don't blindly add the columns to the query, ensure that they are not there first
                            if cur_fk['col'] not in mapped_columns:
                                add_to_query = True
                        except Exception:
                            raise
                    else:
                        # we have a linked table which we don't know where the data should come from and is not in the current mapping. Most probably we are linking different form groups
                        # If the column is mapped to a data source, it most likely means that we need to create this linkage
                        terminal.tprint('\t\tWe have a linked table and we have data for use in the linkage column. Need to create a query to fetch the linked data', 'okblue')
                        cur_table_group['columns'][cur_fk['col']]['is_linked'] = True
                        if cur_fk['fk_table'] == settings.LOOKUP_TABLE:
                            terminal.tprint("\t\tA linkage (%s:%s) to the lookup table, no need to process it in the usual way, just formulate the linkage query" % (table, cur_fk['col']), 'okblue')
                            unique_cols = []
                            # linkage to the lookup table, no need to process it in the usual way, just formulate the linkage query
                            # i don't know where to get the unique columns
                            i = 0
                            for dict_info in cur_table_group['columns'][cur_fk['col']]['dict_info']:
                                cur_table_group['columns'][cur_fk['col']]['dict_info'][i]['linkage'] = {'table': cur_fk['fk_table'], 'col': cur_fk['ref_col'], 'unique_cols': unique_cols}
                                if dict_info['is_a_select'] is True:
                                    cur_table_group['columns'][cur_fk['col']]['dict_info'][i]['linkage']['unique_cols'] = ['form_group', 'parent_node', 't_key']
                                else:
                                    cur_table_group['columns'][cur_fk['col']]['dict_info'][i]['linkage']['unique_cols'] = ['form_group', 't_key']
                                i += 1
                        else:
                            unique_cols = self.get_all_unique_cols(cur_fk['fk_table'])
                            if len(unique_cols) == 0:
                                mssg = "\t\tWe have a linked table, but the table '%s' don't have unique keys that can be used to filter the data. Exiting now..." % cur_fk['table']
                                terminal.tprint(mssg, 'fail')
                                raise Exception(mssg)

                        cur_table_group['columns'][cur_fk['col']]['linkage'] = {'table': cur_fk['fk_table'], 'col': cur_fk['ref_col'], 'unique_cols': unique_cols}

                if add_to_query is True:
                    cur_fk['is_linked_table'] = True
                    if cur_fk['col'] not in mapped_columns:
                        mapped_columns.append(cur_fk['col'])
                        column_names.append(cur_fk['col'])
                        column_values.append("%s")
                    fks_to_add[cur_fk['col']] = cur_fk
                    # self.cur_group_queries[table]['columns'][cur_fk['col']] = cur_fk

        final_query = '%s(%s) VALUES(%s)' % (insert_query, ','.join(column_names), ','.join(column_values))
        if len(duplicate_constraints) == 0:
            # we don't have a constraint for this table, but the columns have not been defined...
            mssg = 'Cowardly refusing to process the data due to <strong>missing unique constraints for the table "%s"</strong>. Please define columns to identify a unique record' % table
            terminal.tprint('\t%s' % mssg, 'fail')
            raise Exception(mssg)
        else:
            is_duplicate_query = 'SELECT %s FROM %s WHERE %s' % (primary_key, table, duplicate_constraints)

        if len(source_datapoints) == 0:
            mssg = 'I did not find the source ODK questions from where to fetch the data from. Quitting now'
            terminal.tprint('\t%s' % mssg, 'fail')
            raise Exception(mssg)

        self.cur_group_queries[table] = cur_table_group
        self.cur_group_queries[table]['query'] = final_query
        self.cur_group_queries[table]['dup_check']['is_duplicate_query'] = is_duplicate_query
        self.cur_group_queries[table]['dup_check']['dup_columns_sources'] = dup_columns_sources
        self.cur_group_queries[table]['source_datapoints'] = source_datapoints
        self.cur_group_queries[table]['dest_columns'] = mapped_columns
        self.cur_group_queries[table]['actual_data_nodes'] = actual_data_nodes

        # add all foreign keys
        all_fks = self.get_all_foreign_keys(table)
        if len(all_fks) != 0:
            for col, cur_fk in six.iteritems(fks_to_add):
                self.cur_group_queries[table]['columns'][col] = cur_fk

        terminal.tprint('\tGenerated query for %s: %s' % (table, final_query), 'ok')

        return False

    def process_form_group_data(self, form_group, is_dry_run, submissions=None):
        """Starts the processing of a form group, whether manual or automatic processing
        Args:
            form_group (string): The name of the form group to process
            is_dry_run (bool): Whether it is a dry run or not
            submissions (array, optional): A list of submission uuids to process
        Returns:
            array: Returns an array (is_error, comments) whether there was an error and the underlying comments
        """
        terminal.tprint("\n\nProcessing the data for form group '%s'" % form_group.group_name, 'okblue')
        comments = []

        if len(FormMappings.objects.filter(form_group=form_group.group_name)) == 0:
            mssg = 'The form group "%s" has no defined mappings.' % form_group.group_name
            terminal.tprint('\t%s' % mssg, 'okblue')
            comments.append(mssg)
            is_error = False
            return is_error, comments

        odk_form = list(ODKForm.objects.filter(form_group=form_group.id).values('id', 'form_id'))[0]

        all_nodes = []
        for (group_name, cur_group) in six.iteritems(self.cur_group_queries):
            all_nodes = copy.deepcopy(all_nodes) + copy.deepcopy(cur_group['source_datapoints'])

        # if we dont have the instance id in the nodes list, include it
        if 'instanceID' not in all_nodes:
            all_nodes.append('instanceID')
        # terminal.tprint(json.dumps(all_nodes), 'fail')
        if submissions is not None:
            terminal.tprint('\tWe have a list of submissions that we need to process manually', 'okblue')
            # form_id, nodes, d_format, download_type, view_name, uuids=None, update_local_data=True, is_dry_run=True
            all_instances = self.fetch_merge_data(odk_form['form_id'], all_nodes, None, 'submissions', None, submissions, True, is_dry_run)
            terminal.tprint(json.dumps(all_instances), 'warn')
        else:
            all_instances = self.fetch_merge_data(odk_form['form_id'], all_nodes, None, 'submissions', None, None, True, is_dry_run)

        terminal.tprint('\tTotal submissions fetched %d' % len(all_instances), 'okblue')

        if not isinstance(all_instances, list):
            terminal.tprint(json.dumps(all_instances), 'okblue')

        is_error = False
        i = 0
        terminal.tprint("\n\nBase preparations are done, now lets save the data for the form group '%s'" % form_group.group_name, 'warn')
        with connections['mapped'].cursor():
            # start a transaction
            for cur_instance in all_instances:
                i += 1
                if is_dry_run:
                    if i > settings.DRY_RUN_RECORDS:
                        terminal.tprint('\tWe have processed the necessary submissions in the group, now breaking...', 'okblue')
                        break

                transaction.set_autocommit(False)
                try:
                    if not isinstance(cur_instance, tuple):
                        terminal.tprint(json.dumps(cur_instance), 'warn')
                    self.save_instance_data(cur_instance, is_dry_run)
                except ValueError as e:
                    comments.append('Value Error: %s' % str(e))
                    continue
                    # is_error = True
                except IntegrityError as e:
                    terminal.tprint('\tIntegrity Error: %s' % str(e), 'fail')
                    comments.append('\tIntegrity Error: %s' % str(e))
                    is_error = True
                    continue
                except Exception as e:
                    terminal.tprint('Error: %s' % str(e), 'fail')
                    comments.append('Error: %s' % str(e))
                    sentry.captureException()
                    is_error = True
                    continue

                if is_dry_run:
                    transaction.rollback()
                else:
                    transaction.commit()
                    # update this record showing that this submission has been processed
                    if re.search('uuid', cur_instance['instanceID']):
                        m = re.findall(r'uuid\:(.+)', cur_instance['instanceID'])
                        raw_subm = RawSubmissions.objects.get(uuid=m[0])
                        raw_subm.is_processed = 1
                        raw_subm.publish()
                        terminal.tprint("\tThe submission '%s' has been processed successfully" % m[0], 'ok')

            # revert the auto commit to true
            transaction.set_autocommit(True)

        terminal.tprint('\tTotal submissions saved %d' % i, 'ok')

        return is_error, comments

    def get_all_foreign_keys(self, table):
        """Given a table, fetches all the foreign keys in the table
        Args:
            table (string): The name of table where the FKs are defined
        Returns:
            array: An array of all the defined foreign keys
        """
        terminal.tprint('\t\tFetching all foreign keys for the table %s' % table, 'okblue')
        fk_query = '''
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '%s' and TABLE_NAME='%s' and REFERENCED_TABLE_SCHEMA is not NULL
        ''' % (settings.DATABASES['mapped']['NAME'], table)
        all_fks = []
        with connections['mapped'].cursor() as mapped_cursor:
            mapped_cursor.execute(fk_query)
            dest_columns = mapped_cursor.fetchall()

            for dest_column in dest_columns:
                all_fks.append({'fk_table': dest_column[4], 'col': dest_column[2], 'ref_col': dest_column[5]})

        # terminal.tprint('\t\tForeign keys for the table %s: %s' % (table, json.dumps(all_fks)), 'okblue')
        return all_fks

    def get_all_unique_cols(self, table):
        terminal.tprint('\t\tFetching all unique keys for the table %s' % table, 'okblue')
        all_unique = []
        with connections['mapped'].cursor() as mapped_cursor:
            dest_columns_q = 'DESC %s' % table
            mapped_cursor.execute(dest_columns_q)
            dest_columns = mapped_cursor.fetchall()

            for dest_column in dest_columns:
                # determine if we have a unique key
                if dest_column[3] == 'UNI':
                    all_unique.append(dest_column[0])
        return all_unique

    def save_instance_data(self, data, is_dry_run):
        """Starts the process of saving an instance to the database
        Args:
            data (json): A JSON object with the data to be saved
            is_dry_run (bool): Description
        Returns:
            None: Returns nothing. Incase of any errors, they will be caught in the try..catch block
        Raises:
            ValueError: Description
        """
        # start the process of saving this instance data
        # the output_structure is required by the process_node function, but we are not using it here, just keep it
        self.output_structure = defaultdict(dict)
        self.cur_fk = {}
        cur_dataset = {}
        # terminal.tprint(json.dumps(self.cur_group_queries), 'ok')
        for table, cur_group in six.iteritems(self.cur_group_queries):
            self.output_structure[table] = ['unique_id']
            self.cur_fk[table] = []

            formatted = self.format_extracted_data(data, cur_group['source_datapoints'])
            cur_dataset[table] = formatted

            for nodes_data in formatted:
                # terminal.tprint('\tNodes: %s' % json.dumps(nodes_data), 'warn')
                try:
                    (data_points, dup_data_points) = self.populate_query(cur_group, nodes_data, data['instanceID'])
                    cursor = connections['mapped'].cursor()

                    for i in range(0, len(data_points)):
                        try:
                            final_query = cur_group['query'] % tuple(data_points[i].values())
                            duplicate_query = cur_group['dup_check']['is_duplicate_query'] % tuple(dup_data_points[i].values())

                            # terminal.tprint('\tExecuting the main query: %s' % final_query, 'ok')
                            cursor.execute(cur_group['query'], tuple(data_points[i].values()))
                            cursor.execute('SELECT @@IDENTITY')
                            last_insert_id = int(cursor.fetchone()[0])
                            self.cur_fk[table].append(last_insert_id)

                        except IntegrityError as e:
                            # it seems we have a duplicate entry, check if it is already saved
                            # terminal.tprint('\tExecuting the duplicate query: %s' % duplicate_query, 'ok')
                            cursor.execute(cur_group['dup_check']['is_duplicate_query'], tuple(dup_data_points[i].values()))
                            inserted_data = cursor.fetchone()
                            if inserted_data is None:
                                # We have a real duplicate data, this is not a logic error but a problem with the source data
                                terminal.tprint("\tDuplicate data, skipping this data record", 'warn')
                                self.create_error_log_entry('duplicate', str(e), data['instanceID'], duplicate_query)
                                continue
                            self.cur_fk[table].append(inserted_data[0])
                        except Exception as e:
                            self.create_error_log_entry('unknown', str(e), data['instanceID'], 'Query: %s, Data: %s' % (cur_group['query'], json.dumps(list(data_points[i].values()))))
                            terminal.tprint('\tError (%s) while executing the final queries.' % str(e), 'fail')
                            sentry.captureException()
                            raise

                        # terminal.tprint('\t%s' % final_query, 'ok')
                except Exception as e:
                    terminal.tprint('\tError (%s) while testing the queries' % str(e), 'fail')
                    # terminal.tprint('\t%s' % final_query, 'okblue')
                    self.create_error_log_entry('data_error', str(e), data['instanceID'], None)
                    sentry.captureException()
                    raise
            terminal.tprint("\tThe instance '%s' has been processed successfully" % data['instanceID'], 'ok')

        return

    def format_extracted_data(self, haystack, data_points):
        formatted = []
        this_node = {}
        found_lists = []
        try:
            for cur_node in list(haystack.keys()):
                if cur_node in data_points:
                    this_node[cur_node] = haystack[cur_node]
                elif isinstance(haystack[cur_node], list) is True:
                    cur_list = self.extract_data_from_list(haystack[cur_node], data_points)
                    # terminal.tprint(json.dumps(cur_list), 'okblue')
                    if len(cur_list) != 0:
                        found_lists.append(cur_list)

            if len(found_lists) != 0:
                for lists in found_lists:
                    for cur_list in lists:
                        new_node = this_node.copy()
                        new_node.update(cur_list)
                        formatted.append(new_node)
            else:
                formatted.append(this_node)

            return formatted
        except AttributeError as e:
            terminal.tprint('\tIncorrect data sent: %s' % str(e), 'fail')
            terminal.tprint('\t%s' % json.dumps(haystack), 'fail')
            return []

    def extract_data_from_list(self, haystack, data_points):
        all_nodes = []
        # terminal.tprint(json.dumps(haystack), 'okblue')
        for cur_list in haystack:
            this_node = {}
            # terminal.tprint(json.dumps(cur_list), 'ok')
            for cur_node in list(cur_list.keys()):
                if cur_node in data_points:
                    this_node[cur_node] = cur_list[cur_node]

            if len(this_node) != 0:
                all_nodes.append(this_node)

        return all_nodes

    def populate_query(self, q_meta, q_data, instanceID):
        """
        Given the destination table structure, corresponding data sets and the rules of the destination table, create a final SQL query that can be used to add data
        Args:
            q_meta (json): An object containing the table structure and definition parameters
            q_data (json): An object containing the extracted dataset to be saved
            instanceID (string): A string containing a unique uuid for the current record
        Returns:
            array: Returns an array of the found data points and duplicate data points
        Raises:
            Exception: Description
            ValueError: If there is an error in processing the `q_data`
        """
        all_data = []
        data_points = OrderedDict()
        dup_data_points = OrderedDict()
        linked_nodes = []
        corresponding_data = []
        for col in q_meta['dest_columns']:
            # get the source node details
            source_node = q_meta['columns'][col]
            # if we have a linked table, the processing logic is very different
            if 'is_linked_table' in source_node:
                if source_node['is_linked_table']:
                    try:
                        # the linked table must have already been processed and the saved id already saved
                        fk_id = self.cur_fk[source_node['fk_table']][0]
                        data_points[col] = fk_id
                        if col in q_meta['dup_check']['dup_columns_sources']:
                            dup_data_points[col] = fk_id
                    except Exception:
                        mssg = "Expecting that the table '%s' is already processed and the generated foreign key saved. I didn't find it. Can't proceed" % source_node['fk_table']
                        terminal.tprint('\t%s' % mssg, 'fail')
                        raise Exception(mssg)
                    continue

            is_foreign_key = True if 'is_foreign_key' in source_node and source_node['is_foreign_key'] else False
            node_data = None

            if 'has_multiple_sources' in source_node and 'is_foreign_key' not in source_node:
                node_data = self.process_multiple_source_node(col, source_node, q_data)
            elif 'regex' in source_node:
                # the data is only coming from one source and we have a regex defined
                node_data = self.validate_data_point(source_node['regex'], q_data[source_node['sources'][0]], source_node['sources'][0])

            # if 'is_geopoint' in source_node:
            #     node_data = self.process_geopoint_node(col, source_node['sources'], q_data)
            if 'is_linked' in source_node:
                # We anticipate multiple sources
                try:
                    if 'dict_info' in source_node:
                        # We have a linkage to the lookup table
                        for dict_info in source_node['dict_info']:
                            # we need to fetch this node data from another table
                            where_criteria = ''
                            no_repeats = 0
                            for criteria in dict_info['linkage']['unique_cols']:
                                this_criteria = '%s=%%s' % criteria
                                where_criteria = '%s and %s' % (where_criteria, this_criteria) if where_criteria != '' else this_criteria
                                no_repeats += 1

                            fetch_query = 'SELECT %s FROM %s WHERE %s' % (dict_info['linkage']['col'], dict_info['linkage']['table'], where_criteria)

                            # Check if we are dealing with a lookup field
                            if 'is_lookup_field' in source_node:
                                # Depending on the question type, the node data as well as method of querying will be different
                                # For selects, we shall have a full lookup table with attribute_value as NULL
                                # For others, we have a partial lookup table where parent_node in the dictionary as NULL and attribute value from the questionnaire
                                if dict_info['is_a_select'] is True:
                                    if dict_info['odk_node'] not in q_data:
                                        # we expect to save the selected value, but for some reason it is not there, continue....
                                        mssg = "\tI couldn't find the entered data for column '%s:%s'. The linkage cannot happen." % (dict_info['form_group'], dict_info['odk_node'])
                                        self.create_error_log_entry('data_error', mssg, instanceID, None)
                                        continue
                                    query_vals = [dict_info['form_group'], dict_info['odk_node'], q_data[dict_info['odk_node']]]
                                else:
                                    query_vals = [dict_info['form_group'], dict_info['odk_node']]

                                odk_source_qst = dict_info['odk_node']
                            else:
                                odk_source_qst = source_node['sources'][0]
                                query_vals = [q_data[odk_source_qst]] * no_repeats

                            cursor = connections['mapped'].cursor()
                            cursor.execute(fetch_query, query_vals)
                            linked_data = cursor.fetchall()
                            if len(linked_data) == 0:
                                # terminal.tprint(json.dumps(q_data), 'fail')
                                mssg = "\tI couldn't find the corresponding dataset in column '%s:%s'. The linkage cannot happen.\n\tQuery: %s\n\tValues: %s" % (source_node['linkage']['table'], source_node['linkage']['col'], fetch_query, json.dumps(query_vals))
                                self.create_error_log_entry('data_error', mssg, instanceID, None)
                                raise ValueError(mssg)
                            elif len(linked_data) > 1:
                                mssg = "\tI found multiple corresponding datasets in column '%s:%s'. The linkage is ambigous.\n\tQuery: %s\n\tValues: %s" % (source_node['linkage']['table'], source_node['linkage']['col'], fetch_query, json.dumps(query_vals))
                                # Multiple entries in the dictionary, but since its the dictionary, just get the first one

                                # self.create_error_log_entry('data_error', mssg, instanceID, None)
                                # raise ValueError(mssg)
                            # terminal.tprint(json.dumps(linked_data[0][0]), 'okblue')
                            linked_nodes.append(linked_data[0][0])

                            # check if we are expecting the companion data....
                            if dict_info['odk_node'] in list(q_meta['actual_data_nodes'].keys()):
                                # we expect to save this node corresponding data in another column
                                corresponding_data.append({
                                    'dest_col': q_meta['actual_data_nodes'][dict_info['odk_node']],
                                    'odk_node': dict_info['odk_node'],
                                    'data': q_data[dict_info['odk_node']]
                                })
                            else:
                                corresponding_data.append(None)

                        # if all is ok, add a placeholder in the nodes data
                        node_data = 'place_holder'
                    else:
                        where_criteria = ''
                        no_repeats = 0
                        for criteria in source_node['linkage']['unique_cols']:
                            this_criteria = '%s=%%s' % criteria
                            where_criteria = '%s and %s' % (where_criteria, this_criteria) if where_criteria != '' else this_criteria
                            no_repeats += 1

                        fetch_query = 'SELECT %s FROM %s WHERE %s' % (source_node['linkage']['col'], source_node['linkage']['table'], where_criteria)

                        odk_source_qst = source_node['sources'][0]
                        query_vals = [q_data[odk_source_qst]] * no_repeats
                        cursor = connections['mapped'].cursor()
                        cursor.execute(fetch_query, query_vals)
                        linked_data = cursor.fetchall()
                        if len(linked_data) == 0:
                            # terminal.tprint(json.dumps(q_data), 'fail')
                            mssg = "\tI couldn't find the corresponding dataset in column '%s:%s'. The linkage cannot happen.\n\tQuery: %s\n\tValues: %s" % (source_node['linkage']['table'], source_node['linkage']['col'], fetch_query, json.dumps(query_vals))
                            self.create_error_log_entry('data_error', mssg, instanceID, None)
                            raise ValueError(mssg)
                        elif len(linked_data) > 1:
                            mssg = "\tI found multiple corresponding datasets in column '%s:%s'. The linkage is ambigous.\n\tQuery: %s\n\tValues: %s" % (source_node['linkage']['table'], source_node['linkage']['col'], fetch_query, json.dumps(query_vals))
                            self.create_error_log_entry('data_error', mssg, instanceID, None)
                            raise ValueError(mssg)
                        # terminal.tprint(json.dumps(linked_data[0][0]), 'okblue')
                        node_data = linked_data[0][0]

                except Exception as e:
                    terminal.tprint('\tError while processing linked data. "%s"' % str(e), 'fail')
                    sentry.captureException()
                    raise
            else:
                odk_source_qst = source_node['sources'][0]

            if is_foreign_key and node_data is None:
                # we need to get the foreign key to this. It must have been processed and saved already
                # terminal.tprint(json.dumps(self.cur_fk), 'warn')
                if source_node['ref_table_name'] not in self.cur_fk:
                    raise Exception("Missing Foreign Key: I don't have a FK to use for table '%s', column '%s'" % (source_node['ref_table_name'], col))
                elif len(self.cur_fk[source_node['ref_table_name']]) == 0:
                    raise Exception("Missing Foreign Key: I don't have a FK to use for table '%s', column '%s'" % (source_node['ref_table_name'], col))
                if len(self.cur_fk[source_node['ref_table_name']]) == 1:
                    # this is definately the foreign key
                    node_data = self.cur_fk[source_node['ref_table_name']][0]
                else:
                    terminal.tprint(json.dumps(self.cur_fk[source_node['ref_table_name']]), 'fail')
                    raise ValueError('Ambigous Foreign Key: I have more than 1 FK to use. I dont know how to proceed')

            if node_data is None:
                # we still dont have the node data, so it must be coming from a single source, nothing special
                try:
                    node_data = q_data[odk_source_qst]
                except Exception:
                    is_nullable = False
                    if 'is_nullable' in source_node:
                        if source_node['is_nullable'] is True:
                            is_nullable = True

                    if is_nullable is True:
                        node_data = None
                    else:
                        mssg = "I can't find the data for the variable '%s' in '%s'" % (odk_source_qst, json.dumps(q_data))
                        raise ValueError(mssg)

            # Append the found node data
            data_points[col] = node_data
            if col in q_meta['dup_check']['dup_columns_sources']:
                dup_data_points[col] = node_data

        # if we have linked nodes, replace the placeholder text with the actual data
        # The data in the linked_nodes is added by a simple append, so there is need to calculate the actual position of where to ge tthe data
        all_data_points = []
        all_dup_data_points = []
        if len(linked_nodes) != 0:
            # terminal.tprint('\Companion data: %s' % json.dumps(corresponding_data), 'fail')
            no_occurences = list(data_points.values()).count('place_holder')
            iterations = int(len(linked_nodes) / no_occurences)
            for i in range(0, iterations):
                dp = OrderedDict()
                ddp = OrderedDict()
                j = 0
                for col, node in six.iteritems(data_points):
                    cur_linked_index = (i * no_occurences) + j
                    if node == 'place_holder':
                        dp[col] = linked_nodes[cur_linked_index]
                        j += 1
                    else:
                        dp[col] = node

                    if col in list(dup_data_points.keys()) and dup_data_points[col] == 'place_holder':
                        ddp[col] = linked_nodes[cur_linked_index]
                    elif col in list(dup_data_points.keys()):
                        ddp[col] = node

                # Update the corresponding data point
                cur_corresponding_data = corresponding_data[i]
                dp[cur_corresponding_data['dest_col']] = cur_corresponding_data['data']
                # Now add the new dict
                all_data_points.append(dp)
                all_dup_data_points.append(ddp)
        else:
            all_data_points = [data_points]
            all_dup_data_points = [dup_data_points]

        # terminal.tprint('\tColumns: %s' % json.dumps(q_meta['dest_columns']), 'fail')
        # terminal.tprint('\n\tData points: %s, Duplicate points: %s' % (json.dumps(all_data_points), json.dumps(all_dup_data_points)), 'okblue')
        return all_data_points, all_dup_data_points

    def process_multiple_source_node(self, column, source_node, q_data):
        # the given multiple input data sources is to be saved to a single column
        # terminal.tprint("\tProcessing column '%s' which has multiple data sources" % column, 'debug')
        nodes_present = 0
        node_data = ''
        for source in source_node['sources']:
            if source in q_data:
                if 'regex' in source_node:
                    self.validate_data_point(source_node['regex'], q_data[source], column)
                nodes_present += 1
                if node_data == '':
                    node_data = q_data[source]
                else:
                    node_data = '%s%s%s' % (node_data, settings.JOINER, q_data[source])

        return node_data

    def process_geopoint_node(self, column, sources, q_data):
        # expecting the source to be only one
        if len(sources) != 1:
            raise Exception("Invalid Mapping: The GPS source data can only be fetched from one source question.")

        # split the data by a space as expected from odk
        geo = q_data[sources[0]].split()

        # try some guessing game which column we are referring to
        if re.search('lat', column):
            # we have a longitude
            return geo[0]
        if re.search('lon', column):
            # we have a longitude
            return geo[1]
        if re.search('alt', column):
            # we have altitude 
            return geo[2]

        raise Exception('Unknown Destination Column: Encountered a GPS data field (%s), but I cant seem to deduce which type(latitude, longitude, altitude) the current column (%s) is.' % (q_data[sources[0]], column))

    def validate_data_point(self, regex, data, column):
        try:
            m = re.findall(r'%s' % regex, data)
            if len(m) == 0:
                raise ValueError("Invalid Data - Column '%s': Found '%s' which does not match the validation criteria '%s' defined" % (column, data, regex))
        except ValueError:
            raise
        except Exception:
            sentry.captureException()
            raise

        return m[0]

    def delete_processed_data(self):
        form_groups = list(ODKFormGroup.objects.all().order_by('order_index'))
        top_error = False
        all_comments = []

        # disable foreign key checks
        cursor = connections['mapped'].cursor()
        cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
        for form_group in form_groups:
            self.truncated_tables = []
            tables = list(FormMappings.objects.filter(form_group=form_group.group_name).values('dest_table_name').distinct())
            for table in tables:
                try:
                    self.truncate_table_data(form_group, table['dest_table_name'], None, None)
                except Exception as e:
                    top_error = True
                    all_comments.append(str(e))
                    terminal.tprint('\t%s' % str(e), 'fail')
                    sentry.captureException()

        # Re-enable the checks
        cursor.execute('SET FOREIGN_KEY_CHECKS = 1')

        # truncate the processing errors too
        with connection.cursor() as cursor1:
            try:
                cursor1.execute('TRUNCATE processing_errors')
                # update the is_processed field in the odk_form table
                RawSubmissions.objects.filter(is_processed=1).update(is_processed=0)
            except Exception as e:
                top_error = True
                all_comments.append(str(e))
                terminal.tprint('\t%s' % str(e), 'fail')
                sentry.captureException()

        return top_error, all_comments

    def truncate_table_data(self, form_group, table, ref_table, ref_column):
        if table in self.truncated_tables:
            terminal.tprint("\tThe table %s has already been truncated, skipping it..." % table, 'okblue')
            return False

        terminal.tprint("\tGenerating %s's (%s group) truncate query" % (table, form_group.group_name), 'okblue')
        truncate_query = "TRUNCATE %s" % table
        mappings = list(FormMappings.objects.filter(form_group=form_group.group_name).filter(dest_table_name=table).values())

        for mapping in mappings:
            if mapping['ref_table_name'] is not None and table != mapping['dest_table_name']:
                # this table should be truncated before the current table
                self.truncate_table_data(form_group, mapping['dest_table_name'], table, mapping['dest_column_name'])

        terminal.tprint("\tTruncating the table '%s'" % table, 'warn')
        cursor = connections['mapped'].cursor()
        cursor.execute(truncate_query)
        self.truncated_tables.append(table)

        return False

    def get_table_primary_key(self, table):
        with connections['mapped'].cursor() as mapped_cursor:
            dest_columns_q = 'DESC %s' % table
            mapped_cursor.execute(dest_columns_q)
            dest_columns = mapped_cursor.fetchall()

            for dest_column in dest_columns:
                # check if it is a primary key
                if dest_column[3] == 'PRI':
                    return dest_column[0]

        return None

    def create_error_log_entry(self, e_type, err_message, uuid, comments):
        comments = '' if comments is None else comments

        if re.match('^uuid', uuid):
            m = re.findall("^uuid\:(.+)$", uuid)
            uuid = m[0]

        try:
            proc_err = ProcessingErrors(
                err_code=settings.ERR_CODES[e_type]['CODE'],
                err_message=err_message,
                data_uuid=uuid,
                err_comments=comments,
                is_resolved=0
            )
            proc_err.publish()
        except Exception as e:
            mssg = '%s, %s, "%s" %s %s' % (str(e), e_type, err_message, uuid, comments)
            terminal.tprint(mssg, 'fail')
            sentry.captureException()
            raise Exception(mssg)

    def fetch_processing_status(self, cur_page, per_page, offset, sorts, queries):
        """
        Fetch the processing status of all the forms
        @todo: Proper pagination of the results
        Args:
            cur_page (TYPE): Description
            per_page (TYPE): Description
            offset (TYPE): Description
            sorts (TYPE): Description
            queries (TYPE): Description
        Returns:
            array: Returns an array with the processing status and a JSON of the form status
        """
        with connection.cursor() as cursor:
            form_details_q = 'SELECT b.form_id, b.id, b.form_name, date_format(b.date_created, "%d %b %Y") as date_created, c.group_name, a.is_processed, count(*) as r_count FROM raw_submissions as a INNER JOIN odkform as b on a.form_id=b.id INNER JOIN form_groups as c on b.form_group_id=c.id GROUP BY b.id, a.is_processed ORDER BY c.group_name, b.form_id, a.is_processed'
            cursor.execute(form_details_q)
            form_details = self.dictfetchall(cursor)

            to_return = {}
            for res in form_details:
                r_sub = RawSubmissions.objects.filter(form_id=res['id']).order_by('-submission_time')
                if len(r_sub) == 0:
                    last_sub_time = 'N/A'
                else:
                    last_sub = model_to_dict(r_sub[0])
                    last_sub_time = last_sub['submission_time']
                
                if res['form_id'] not in to_return:
                    to_return[res['form_id']] = {
                        'form_id': res['form_id'],
                        'form_name': res['form_name'],
                        'form_group': res['group_name'],
                        'date_created': res['date_created'],
                        'last_submission_date': last_sub_time,
                        'no_submissions': 0,
                        'no_processed': res['r_count'] if res['is_processed'] == 1 else 0,
                        'unprocessed': res['r_count'] if res['is_processed'] == 0 else 0
                    }
                else:
                    if res['is_processed'] == 1:
                        to_return[res['form_id']]['no_processed'] += res['r_count']
                    elif res['is_processed'] == 0:
                        to_return[res['form_id']]['unprocessed'] += res['r_count']

                to_return[res['form_id']]['no_submissions'] += res['r_count']

        return_this = []
        for form_id, details in six.iteritems(to_return):
            # "{0:0.2f}".format(loc[1])
            # details['perc_error'] = "{:.2f}".format(((details['no_submissions'] - details['no_processed']) / details['no_submissions']) * 100)
            # details['perc_error'] = int(details['unprocessed']) / int(details['no_submissions'])
            details['perc_error'] = details['unprocessed']
            # details['perc_error'] = 3
            return_this.append(details)

        return False, {'records': return_this, "queryRecordCount": len(return_this), "totalRecordCount": len(return_this)}

    def processing_errors(self, cur_page, per_page, offset, sorts, queries):
        all_errors = ProcessingErrors.objects.all().order_by('-id')
        p = Paginator(all_errors, per_page)
        p_errors = p.page(cur_page)
        if sorts is not None:
            print(sorts)

        to_return = []
        for error in p_errors:
            err = model_to_dict(error)
            err_status = ('success', 'Resolved') if err['is_resolved'] else ('danger', 'Pending')
            err['status'] = '<button type="button" class="btn btn-sm btn-outline btn-%s">%s</button>' % err_status
            err['actions'] = '<button type="button" data-identifier="%s" class="edit_record btn btn-sm btn-outline btn-warning">View</button>' % err['id']
            to_return.append(err)
        return False, {'records': to_return, "queryRecordCount": p.count, "totalRecordCount": p.count}

    def fetch_single_error(self, err_id):
        error = ProcessingErrors.objects.all().filter(id=err_id)
        cur_error = model_to_dict(error[0])

        # if the uuid has the string uuid, remove it
        if re.match('^uuid', cur_error['data_uuid']):
            m = re.findall("^uuid\:(.+)$", cur_error['data_uuid'])
            uuid = m[0]
        else:
            uuid = cur_error['data_uuid']

        # get the record with this uuid
        r_sub = RawSubmissions.objects.all().filter(uuid=uuid)
        r_sub = model_to_dict(r_sub[0])

        return False, cur_error, r_sub

    def fetch_base_map_settings(self):
        with connection.cursor() as cursor:
            tz_details_q = 'SELECT id, iso_code, name, center_lat, center_long FROM country where iso_code = "TZA"'
            cursor.execute(tz_details_q)
            tz_details = self.dictfetchall(cursor)

            terminal.tprint(json.dumps(tz_details), 'warn')

        return tz_details[0]

    def dictfetchall(self, cursor):
        # Return all rows from a cursor as a dict
        columns = [col[0] for col in cursor.description]
        return [
            dict(list(zip(columns, row)))
            for row in cursor.fetchall()
        ]

    def first_level_geojson(self, c_code):
        with connection.cursor() as cursor:
            tz_details_q = 'SELECT a.id, a.iso_code, a.name, a.center_lat, a.center_long, b.geometry FROM country as a inner join admin_level_one as b on a.id=b.country_id where a.id = %d' % c_code
            cursor.execute(tz_details_q)
            tz_details = self.dictfetchall(cursor)

            to_return = []
            for res in tz_details:
                to_return.append({
                    'c_iso': res['iso_code'],
                    'c_name': res['name'],
                    'c_lat': res['center_lat'],
                    # 'c_perc': res.c_perc,
                    'c_lon': res['center_long'],
                    'polygon': json.loads(res['geometry'])
                })

        return to_return

    def save_json_edits(self, err_id, json_data):
        terminal.tprint(json.dumps(json_data), 'debug')
        try:
            cur_error = list(ProcessingErrors.objects.filter(id=err_id).values('data_uuid'))[0]
        except Exception as e:
            terminal.tprint("\tError! Couldn't find the defined processing error with id %s. \n\t%s" % (err_id, str(e)), 'fail')
            return 1, "Could not find the defined processing error."

        # if the uuid has the string uuid, remove it
        if re.match('^uuid', cur_error['data_uuid']):
            m = re.findall("^uuid\:(.+)$", cur_error['data_uuid'])
            uuid = m[0]
        else:
            uuid = cur_error['data_uuid']

        # get the record with this uuid
        subm = RawSubmissions.objects.get(uuid=uuid)
        subm.raw_data = json_data
        subm.is_modified = 1
        subm.publish()

        return 0, "The submission has been saved successfully."

    def process_single_submission(self, err_id):
        try:
            cur_error = list(ProcessingErrors.objects.filter(id=err_id).values('data_uuid'))[0]
        except Exception as e:
            terminal.tprint("\tError! Couldn't find the defined processing error with id %s. \n\t%s" % (err_id, str(e)), 'fail')
            return True, "Could not find the defined processing error."

        # if the uuid has the string uuid, remove it
        if re.match('^uuid', cur_error['data_uuid']):
            m = re.findall("^uuid\:(.+)$", cur_error['data_uuid'])
            uuid = m[0]
        else:
            uuid = cur_error['data_uuid']

        (is_error, comments) = self.manual_process_data(False, [uuid])
        if is_error is False:
            cur_error = ProcessingErrors.objects.get(id=err_id)
            cur_error.is_resolved = 1
            cur_error.publish()

        return is_error, comments

    def get_odk_forms_info(self, cur_page, per_page, offset, sorts, queries):
        """
        Get all the defined ODK forms
        """
        # check if there are new forms first
        self.refresh_forms()
        all_forms = ODKForm.objects.select_related('form_group').values('id', 'form_group__group_name', 'form_id', 'form_name', 'full_form_id', 'auto_update', 'is_source_deleted').order_by('id')
        # print (ODKForm.objects.select_related('form_group').values('id', 'form_group__order_index', 'form_group__group_name', 'form_id', 'form_name', 'full_form_id', 'auto_update', 'is_source_deleted').order_by('id').query)
        p = Paginator(all_forms, per_page)
        p_forms = p.page(cur_page)
        if sorts is not None:
            print(sorts)

        to_return = []
        for frm in p_forms:
            frm['form_group'] = frm['form_group__group_name']
            frm['actions'] = '<button type="button" data-form_id="%s" class="edit_form btn btn-sm btn-outline btn-warning">Edit</button>' % frm['id']
            to_return.append(frm)
        return False, {'records': to_return, "queryRecordCount": p.count, "totalRecordCount": p.count}

    def get_odk_forms_list(self):
        """
        Get all the defined ODK forms
        """
        # check if there are new forms first
        self.refresh_forms()
        all_forms = ODKForm.objects.select_related('form_group').all().values('id', 'form_id', 'form_group__group_name', 'form_name', 'full_form_id', 'auto_update', 'is_source_deleted').order_by('id')

        to_return = []
        for frm in all_forms:
            # frm = model_to_dict(form)
            frm['form_group'] = frm.form_group__group_name
            to_return.append(frm)
        return to_return

    def save_settings(self, request):
        """
        Saves the user settings to the database
        """
        try:
            for key in request.POST:
                if key == 'csrfmiddlewaretoken' or key == 'dest_db_confirm_password':
                    continue
                if request.POST[key] != '':
                    # check if the setting exists, if it does, update it, else add it
                    cur_setting = SystemSettings.objects.filter(setting_key=key)
                    if len(cur_setting) == 0:
                        cur_setting = SystemSettings(
                            setting_key=key,
                            setting_value=request.POST[key]
                        )
                        cur_setting.publish()
                    else:
                        cur_setting = SystemSettings.objects.get(setting_key=key)
                        cur_setting.setting_value = request.POST[key]
                        # now update
                        cur_setting.publish()
        except Exception as e:
            terminal.tprint(str(e), 'ok')
            sentry.captureException()
            return {'error': True, 'message': str(e)}

        return {'error': False, 'mappings': 'The settings were saved successfully'}

    def get_all_settings(self):
        """
        Get all the defined system settings
        Returns:
            list: Returns a list of all the defined system settings
        """
        all_settings = list(SystemSettings.objects.all().values('setting_key', 'setting_value'))
        to_return = {}
        for setting in all_settings:
            to_return[setting['setting_key']] = setting['setting_value']

        return to_return

    def is_first_login(self):
        """
        Check if this is the first time that the user is loggin in
        Returns:
            boolean: Returns True if it is the first time the user is logging in the system else returns false
        """
        system_name = SystemSettings.objects.filter(setting_key='system_name')

        return True if len(system_name) == 0 else False

    def fetch_form_details(self, form_id):
        form = ODKForm.objects.all().filter(id=form_id)
        cur_form = model_to_dict(form[0])

        return False, cur_form

    def save_form_details(self, request):
        try:
            form_id = int(request.POST['form_id'])
            form = ODKForm.objects.get(form_id=form_id)

            group_id = int(request.POST['group_name'])
            if group_id == -1:
                form.form_group = None
            else:
                form.form_group = ODKFormGroup.objects.get(id=group_id)
            form.auto_update = True if request.POST['auto_update'] == 'yes' else False
            form.is_source_deleted = True if request.POST['is_source_deleted'] == 'yes' else False
            form.publish()

            return False, 'The form settings were saved successfully'
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            return True, 'There was an error while saving the form settings'

    def fetch_form_groups(self):
        try:
            groups = ODKFormGroup.objects.all()

            all_groups = []
            for frm_group in groups:
                all_groups.append(model_to_dict(frm_group))
        except Exception as e:
            terminal.tprint(str(e))
            return True, 'There was an error while fetching data from the database'

        return False, all_groups

    def get_form_groups_info(self, cur_page, per_page, offset, sorts, queries):
        """
        Get all the defined form groups
        """
        all_groups = ODKFormGroup.objects.all().values('id', 'order_index', 'group_name', 'comments').order_by('id')
        p = Paginator(all_groups, per_page)
        f_groups = p.page(cur_page)
        if sorts is not None:
            print(sorts)

        to_return = []
        for grp in f_groups:
            grp['actions'] = "<a class='edit_group' data-group_id='%d'>Edit Group</a>" % (grp['id'])
            to_return.append(grp)
        return False, {'records': to_return, "queryRecordCount": p.count, "totalRecordCount": p.count}

    def save_group_details(self, request):
        try:
            group_id = request.POST['group_id']
            group_name = request.POST['group_name']
            group_index = request.POST['group_index']
            comments = request.POST['comments']

            if group_id != '':
                group_id = int(group_id)
                group = ODKFormGroup.objects.get(id=group_id)
            else:
                group = ODKFormGroup()

            group.order_index = group_index
            group.group_name = group_name
            group.comments = comments

            group.publish()

            return False, 'The group settings were saved successfully'
        except Exception as e:
            terminal.tprint(str(e), 'fail')
            return True, 'There was an error while saving the group settings'

    def auto_create_form_group(self, full_form_id):
        # we are expecting the full form id to have the version number at the end separated with an underscore
        # eg. my_awesome_name_v1, my_awesome_name_v14
        # we process this and extract the my_awesome_name as form name
        try:
            if re.match('^(.+)(_v\d+)(_\d+)?$', full_form_id) is None:
                raise ValueError("The supplied form id '%s' doesn't conform to the expected pattern like 'my_awesome_name_v14' or 'my_awesome_name_v14_2'" % full_form_id)

            form_group_name = re.findall('^(.+)(_v\d+)(_\d+)?$', full_form_id)[0][0]

            # check if the group exists first before saving it
            group = ODKFormGroup.objects.filter(group_name=form_group_name).first()
            if group is None:
                group = ODKFormGroup(
                    order_index=None,
                    group_name=form_group_name,
                    comments="Auto created from '%s'" % full_form_id
                )
                group.publish()
            return group

        except Exception as e:
            terminal.tprint(str(e), 'ok')
            sentry.captureException()
            return None
