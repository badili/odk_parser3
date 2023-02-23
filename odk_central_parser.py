import requests
import re
import os
import json
import xmltodict
import six

from django.conf import settings

from .models import ODKFormGroup, ODKForm, RawSubmissions

from .terminal_output import Terminal
terminal = Terminal()

class OdkCentral():
    # lets define the endpoints for ODK central
    cur_user_details = '%(url)s/v1/users/current'
    list_all_projects = '%(url)s/v1/projects'
    list_all_project_forms = '%(url)s/v1/projects/%(project_id)d/forms'
    list_all_form_submissions = '%(url)s/v1/projects/%(project_id)d/forms/%(form_name)s/submissions'
    fetch_single_xml_submission = '%(url)s/v1/projects/%(project_id)d/forms/%(form_name)s/submissions/%(uuid)s.xml'
    fetch_json_submissions = '%(url)s/v1/projects/%(project_id)d/forms/%(form_name)s.svc/table?$wkt=true'
    submission_count = '%(url)s/v1/projects/%(project_id)d/forms/%(form_name)s.svc/Submissions?$top=0&$count=true'
    form_structure = '%(url)s/v1/projects/%(project_id)d/forms/%(form_id)s.xml'

    def __init__(self, server_url, app_username, app_password):
        self.server_url = server_url
        self.app_username = app_username
        self.app_password = app_password

        # try and validate the credentials
        try:
            url = self.cur_user_details % {'url': self.server_url}
            response = self.process_curl_request(url)
            user_data = response.json()

        except Exception as e:
            raise

        self.odk_user_id = user_data['id']
        self.odk_user_email = user_data['email']
        self.odk_user_name = user_data['displayName']

    def process_curl_request(self, url, stream_data=False, req_type = 'GET'):
        """
        Create and execute a curl request
        """ 
        try:
            if req_type == 'GET':
                r = requests.get(url, auth=(self.app_username, self.app_password), stream=stream_data)
            elif req_type == 'POST':
                r = requests.post(url, auth=(self.app_username, self.app_password), stream=stream_data)
            else:
                raise Exception('Unsupported request type')

            terminal.tprint('\tExecuting %s ....' % url, 'debug')
            
            if r.status_code == 200:
                # return r.iter_content(chunk_size=None, decode_unicode=True)
                return r

            elif r.status_code == 401:
                raise Exception("Invalid username or password")

            elif r.status_code == 404:
                terminal.tprint("\t%d: Form not found" % r.status_code, 'fail')
                return None
            else:
                if settings.DEBUG:
                    terminal.tprint("\tResponse %d" % r.status_code, 'fail')
                    terminal.tprint(r.text, 'fail')
                    terminal.tprint(url, 'warn')
                return None

        except ConnectionError as e:
            raise ConnectionError('There was an error while connecting to the ONA server. %s' % str(e))

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            raise

    def process_user_projects(self):
        # get all projects that the current user is authorized
        url = self.list_all_projects % {'url': self.server_url}
        response = self.process_curl_request(url)
        projects = response.json()

        for proj in projects:
            # process this project
            if settings.DEBUG: terminal.tprint('\tProcessing %s' % proj['name'], 'debug')
            self.save_project(proj['name'], proj['id'])
            self.process_user_project(proj['id'])

    def process_user_project(self, project_id):
        url = self.list_all_project_forms % {'url': self.server_url, 'project_id': project_id}
        response = self.process_curl_request(url)
        all_forms = response.json()

        for form in all_forms:
            # process this form
            # skip closed forms
            if form['state'] != 'open' or form['draftToken']: continue
            # first get the number of submissions
            url = self.submission_count % {'url': self.server_url, 'project_id': project_id, 'form_name': form['xmlFormId']}
            response = self.process_curl_request(url)
            subm_count = response.json()['@odata.count']

            self.save_form(project_id, form, subm_count)
            self.process_form(project_id, form['xmlFormId'])

    def process_form(self, project_id, form_name):
        # check if the current form is saved

        # get number of submissions of this form
        subm_count = self.get_submissions_count(project_id, form_name)

        terminal.tprint('\tThe form %s has %d submissions' % (form_name, subm_count), 'debug')

        # get all submissions
        subm_url = self.list_all_form_submissions % {'url': self.server_url, 'project_id': project_id, 'form_name': form_name}
        response = self.process_curl_request(subm_url)
        subms_meta = response.json()

        for subm in subms_meta:
            xml_url = self.fetch_single_xml_submission % {'url': self.server_url, 'project_id': project_id, 'form_name': form_name, 'uuid': subm['instanceId']}
            response = self.process_curl_request(xml_url, True)
            raw_xml = response.content.decode('utf-8')

            t_submission = RawSubmissions(
                form=self.cur_form,
                # it seems some submissions don't have a uuid returned with the submission. Use our previous uuid
                uuid=subm['instanceId'],
                duration=0,
                is_processed=0,
                is_modified=0,
                submission_time=subm['createdAt'],
                raw_data=xmltodict.parse(raw_xml, process_namespaces=True)['data']
            )
            t_submission.full_clean()
            t_submission.save()
            # raise Exception('Testing')


        # terminal.tprint(json.dumps(all_forms), 'fail')
        # raise Exception('Testing')

    def save_project(self, project_name, project_id):
        # In ODK central we have a project, but in ona we didn't have. The project is roughly equal to
        # form group

        # Forms group is not valid for ODK central forms, but to maintain backward compatibility
        # we add a project name as part of form groups. 
        # In ODK central form_group have a 1:1 relationship with forms

        try:
            self.cur_project = ODKFormGroup.objects.get(form_project=project_name)

        except ODKFormGroup.DoesNotExist:
            if settings.DEBUG: terminal.tprint("\tThe group/project '%s' doesn't exist. Creating a new group..." % project_name, 'debug')
            new_group = ODKFormGroup(
                group_name=project_name,
                form_project=project_name,
                project_id=project_id
            )
            new_group.full_clean()
            new_group.save()

            self.cur_project = new_group

    def save_form(self, project_id, form_details, subm_count):
        try:
            self.cur_form = ODKForm.objects.get(full_form_id=form_details['xmlFormId'])

        except ODKForm.DoesNotExist:
            # the form does not exist, lets create a new form
            if settings.DEBUG: terminal.tprint("\tThe form '%s' (%s) doesn't exist. Creating a new form..." % (form_details['name'], form_details['xmlFormId']), 'debug')

            new_form = ODKForm(
                form_id=form_details['sha256'],
                form_group=self.cur_project,
                form_name=form_details['name'],
                full_form_id=form_details['xmlFormId'],
                no_submissions=subm_count,
                is_active=1,
                datetime_published=form_details['publishedAt']
            )
            new_form.full_clean()
            new_form.save()

            self.cur_form = new_form

    def get_submissions_count(self, project_id, form_name):
        url = self.submission_count % {'url': self.server_url, 'project_id': project_id, 'form_name': form_name}
        response = self.process_curl_request(url)
        subm_count = response.json()['@odata.count']

        return subm_count

    def get_form_structure(self, form_id):
        cur_form = ODKForm.objects.select_related('form_group').values('form_group__project_id', 'full_form_id').get(form_id=form_id)
        xml_url = self.form_structure % {'url': self.server_url, 'form_id': cur_form['full_form_id'], 'project_id': cur_form['form_group__project_id']}

        response = self.process_curl_request(xml_url, True)
        raw_xml = response.content.decode('utf-8')
        raw_data = xmltodict.parse(raw_xml, process_namespaces=True)
        self.process_odk_central_form(raw_data)

    def process_odk_central_form(self, raw_data):
        raw_structure = raw_data['http://www.w3.org/1999/xhtml:html']['http://www.w3.org/1999/xhtml:body']
        for key, value in six.iteritems(raw_structure):
            clean_key = self.clean_json_key(key)
            if clean_key == 'class': continue
            self.process_form_node(clean_key, clean_key, value)

    def process_form_node(self, node_type, n_key, node_items):
        for cur_key, cur_value in node_items:
            print(cur_value)

    def clean_json_key(self, j_key):
        # given a key from ona with data, get the sane(last) part of the key
        m = re.findall("/?([\.\w\-]+)$", j_key)
        return m[0]







