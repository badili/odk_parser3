import requests
import traceback
import re

from cryptography.fernet import Fernet
from django.conf import settings
from raven import Client

from .terminal_output import Terminal

terminal = Terminal()
sentry = Client(settings.SENTRY_DSN)

class Onadata():
    """
    Contains the class for onadata integration
    """
    def __init__(self, server_url, token=None):
        self.server = server_url
        self.api_token = token
        self.headers = {'Authorization': "Token %s" % self.api_token}

        # endpoints
        self.api_all_forms = 'api/v1/forms'
        self.form_data = 'api/v1/data/'
        self.form_stats = 'api/v1/stats/submissions/'
        self.form_rep = 'api/v1/forms/'
        self.orgs = 'api/v1/orgs'
        self.media = 'api/v1/media'
        self.metadata_uri = 'api/v1/metadata'
        self.initiate_paswd_reset = 'api/v1/user/reset_url'
        self.finalize_paswd_reset = 'api/v1/user/reset'
        self.share_url = 'api/v1/forms/%d/share'
        # self.reset_url = '%s/%s' % (self.server, 'reset_form')
        # this is hardccoded by ona
        self.reset_url = 'http://testdomain.com/reset_form'

    def process_curl_request(self, url):
        """
        Create and execute a curl request
        """
        # terminal.tprint("Processing API request %s" % url, 'okblue')
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)

        except Exception as e:
            print((traceback.format_exc()))
            terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while processing an Onadata request')

    def register_new_profile(self, user_details):
        """
        Register a new profile
        """
        print('Registering a new user')
        try:
            url = '%s/%s' % (self.server, 'api/v1/profiles')
            r = requests.post(url, user_details, headers=self.headers)
            if r.status_code == 201:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            print((traceback.format_exc()))
            terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while registering a new profile')

    def register_organization(self, org_details):
        """
        Register a new project
        """
        print('Registering a new organization')
        # requires admin privileges
        try:
            url = '%s%s' % (self.server, 'api/v1/projects')
            r = requests.post(url, project_details, headers=self.headers)
            if r.status_code == 201:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while registering a new profile')

    def create_project(self, project_details):
        """
        Register a new project
        """
        print('Registering a new project')
        try:
            url = '%s%s' % (self.server, 'api/v1/projects')
            r = requests.post(url, project_details, headers=self.headers)
            if r.status_code == 201:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while registering a new profile')

    def upload_itemsets_csv(self, file_name, resource_name, prefix):
        # for all projects which are downloadable, upload the new itemsets.csv
        # print('uploading an itemsets csv')
        try:
            url = "%s%s" % (self.server, self.api_all_forms)
            all_forms = self.process_curl_request(url)
            # terminal.tprint(json.dumps(all_forms), 'fail')
            if all_forms is None:
                raise Exception(("Error while executing the API request %s" % url))

            for form in all_forms:
                if not form['downloadable']: continue

                # testing purposes
                # if not (form['id_string'] == 'testing_v0_1' or form['id_string'] == 'chickens_v9_7'): continue
                if re.match(prefix, form['id_string']) is None:
                    # skip things we are not interested in
                    continue

                # we are updating this form
                # we might need to delete the existing metadata
                # print("\n\nupdating %s %s" % (form['id_string'], resource_name))

                # check if we have metadata
                meta_url = '%s%s?xform=%s' % (self.server, self.metadata_uri, form['formid'])
                meta_r = requests.get(meta_url, headers=self.headers)
                # print("Fetching meta response code %s" % meta_r.status_code)
                if meta_r.status_code != 200:
                    terminal.tprint("Response %d: %s" % (meta_r.status_code, meta_r.text), 'fail')
                    raise Exception(meta_r.text)

                for form_meta in meta_r.json():
                    if form_meta['data_value'] == resource_name:
                        # we need to delete this media
                        # print("We found an old media, '%s', deleting it..." % form_meta['data_value'])
                        delete_url = '%s%s/%s' % (self.server, self.metadata_uri, form_meta['id'])
                        # to delete a metadata, I need super privileges, something I can't figure out for now
                        # so lets use the master token
                        master_headers = {'Authorization': "Token %s" % settings.ONADATA_MASTER}
                        del_r = requests.delete(delete_url, headers=master_headers)

                        if del_r.status_code != 204:
                            # something went wrong
                            raise Exception(del_r.text)
                
                # print("Deleting response code %s" % del_r.status_code)

                url = '%s%s' % (self.server, self.metadata_uri)
                itemsets = {'data_file': open(file_name, 'rt')}
                payload = {'data_type': 'media', 'data_value': resource_name, 'xform': form['formid']}

                r = requests.post(url, files=itemsets, data=payload, headers=self.headers)
                # print("Media update response code %s " % r.status_code)
                
                if r.status_code != 201:
                    # terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                    raise Exception(r.text)
        
        except ConnectionError as e:
            sentry.captureException()
            raise Exception("%s\n%s" % ("We can't establish a connection to the onadata server", str(e)))
        
        except Exception as e:
            sentry.captureException()
            raise Exception(e)

    def reset_ona_password(self, email, new_password):
        try:
            # 1. Get the user id and the reset token
            # 2. Reset the password
            url = '%s/%s' % (self.server, self.initiate_paswd_reset)
            user_details = {'email': email, 'reset_url': self.reset_url}
            r = requests.post(url, user_details, headers=self.headers)
            if r.status_code != 200:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)

            # we should have our uid and token
            resp = r.json()
            url = '%s/%s' % (self.server, self.finalize_paswd_reset)
            user_details = {'new_password': new_password, 'uid': resp['uid'], 'token': resp['token']}
            r = requests.post(url, user_details, headers=self.headers)
            if r.status_code != 200:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)

            # we are good, password reset successfully

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while processing an Onadata request')

    def share_project_forms(self, form_prefix_regex, share_details):
        # form_prefixes is a list of prefix to use
        # users is a list of usernames to share the form with
        # role is the role to use: It can be readonly, dataentry, editor, manager

        try:
            url = "%s/%s" % (self.server, self.api_all_forms)
            print(url)
            all_forms = self.process_curl_request(url)

            if all_forms is None:
                raise Exception(("Error while executing the API request %s" % url))

            for form in all_forms:
                if not form['downloadable']: continue

                # testing purposes
                # if not (form['id_string'] == 'testing_v0_1' or form['id_string'] == 'chickens_v9_7'): continue
                if re.match(form_prefix_regex, form['id_string']) is None:
                    # skip things we are not interested in
                    continue

                # we are sharing this form with the users
                # share_url = '%s%s' % (self.server, self.metadata_uri)

                share_url = '%s/%s' % (self.server, self.share_url % form['formid'])
                for type_, det in share_details.items():
                    det['usernames'] = ','.join(det['usernames'])
                    req = requests.post(share_url, data=det, headers=self.headers)

                    if req.status_code != 204:
                        # something went wrong
                        raise Exception(req.text)

        
        except ConnectionError as e:
            sentry.captureException()
            raise Exception("%s\n%s" % ("We can't establish a connection to the onadata server", str(e)))
        

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while sharing a form with users')
