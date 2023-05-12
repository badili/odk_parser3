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
        self.project_share_url = 'api/v1/projects/%d/share'
        self.user = 'api/v1/user'
        self.project_forms = 'api/v1/projects/%d/forms'
        self.change_password = 'api/v1/profiles/%s/change_password'
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
        if settings.DEBUG: print('Registering a new user')
        try:
            url = '%s/%s' % (self.server, 'api/v1/profiles')
            # print('Executing the url %s' % url)
            r = requests.post(url, user_details, headers=self.headers)
            if r.status_code == 201:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while registering a new profile')

    def register_organization(self, org_details, api_token):
        """
        Register a new project
        """
        if settings.DEBUG: print('Registering a new organization')
        # requires admin privileges
        try:
            url = '%s/%s' % (self.server, 'api/v1/projects')
            xls_headers = {'Authorization': "Token %s" % api_token}
            r = requests.post(url, org_details, headers=xls_headers)
            if settings.DEBUG: print(r.json())
            if r.status_code == 201:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while registering a new organization')

    def create_project(self, project_details, api_token):
        """
        Register a new project
        """
        if settings.DEBUG: print('Registering a new project')
        try:
            url = '%s/%s' % (self.server, 'api/v1/projects')
            xls_headers = {'Authorization': "Token %s" % api_token}
            r = requests.post(url, project_details, headers=xls_headers)
            if settings.DEBUG: print(r.json())
            if r.status_code == 201:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while registering a new profile')

    def upload_itemsets_csv(self, file_name, resource_name, form_prefixes):
        # for all projects which are downloadable, upload the new itemsets.csv
        # print('uploading an itemsets csv')
        try:
            url = "%s/%s" % (self.server, self.api_all_forms)
            all_forms = self.process_curl_request(url)
            if settings.DEBUG: terminal.tprint('Checking all forms -- %s' % url, 'debug')
            # terminal.tprint(json.dumps(all_forms), 'fail')
            if all_forms is None:
                raise Exception(("Error while executing the API request %s" % url))

            for form in all_forms:
                if not form['downloadable']: continue

                # check if the form id matches one of our form prefixes
                z = lambda x: re.match(x, form['id_string'])
                if settings.DEBUG: print("\nEvaluating %s" % form['id_string'])
                if settings.DEBUG: print(list(map(z, form_prefixes)))
                if len(list(filter(None, map(z, form_prefixes)))) == 0: continue            # the form is not in our list

                # if settings.DEBUG: print("Updating %s" % form['id_string'])
                # we are updating this form
                # we might need to delete the existing metadata
                # print("\n\nupdating %s %s" % (form['id_string'], resource_name))

                # check if we have metadata
                meta_url = '%s/%s?xform=%s' % (self.server, self.metadata_uri, form['formid'])
                meta_r = requests.get(meta_url, headers=self.headers)
                # print("Fetching meta response code %s" % meta_r.status_code)
                if meta_r.status_code != 200:
                    terminal.tprint("Response %d: %s" % (meta_r.status_code, meta_r.text), 'fail')
                    raise Exception(meta_r.text)

                for form_meta in meta_r.json():
                    if form_meta['data_value'] == resource_name:
                        # we need to delete this media
                        # print("We found an old media, '%s', deleting it..." % form_meta['data_value'])
                        delete_url = '%s/%s/%s' % (self.server, self.metadata_uri, form_meta['id'])
                        # to delete a metadata, I need super privileges, something I can't figure out for now
                        # so lets use the master token
                        master_headers = {'Authorization': "Token %s" % settings.ONADATA_MASTER}
                        del_r = requests.delete(delete_url, headers=master_headers)

                        if del_r.status_code != 204:
                            # something went wrong
                            raise Exception(del_r.text)
                
                # print("Deleting response code %s" % del_r.status_code)

                url = '%s/%s' % (self.server, self.metadata_uri)
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
            if settings.DEBUG: print(url)
            all_forms = self.process_curl_request(url)
            share_errors = []

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
                for det in share_details:
                    req = requests.post(share_url, data=det, headers=self.headers)

                    if req.status_code != 204:
                        # something went wrong
                        share_errors.append(req.text)

            if len(share_errors) != 0:
                sentry.captureMessage("There was an error while sharing some forms. %s" % '; '.join(share_errors), level='debug', extra={'forms': form_prefix_regex, 'share_details': share_details})
                return 'There was some errors while sharing some forms with some users. If some users cannot see some forms, please contact the system admin.'
            else:
                return None

        
        except ConnectionError as e:
            sentry.captureException()
            raise Exception("%s\n%s" % ("We can't establish a connection to the onadata server", str(e)))
        

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while sharing a form with users')

    def get_audit_log_file(self):
        # The user audit log file is located on the server media folder.
        # There is a table logger_attachment in the database which contains a list of all audit logs and their associated instances
        # onadata=# select * from logger_attachment order by id desc limit 5;
        #   id  |                     media_file                      |          mimetype           | extension | instance_id |         date_created          |         date_modified         |          deleted_at           | file_size |   name    |  deleted_by_id
        # ------+-----------------------------------------------------+-----------------------------+-----------+-------------+-------------------------------+-------------------------------+-------------------------------+-----------+-----------+---------------
        #  1031 | kws/attachments/56_kws_immob_v1_0/audit_8FKg0eJ.csv | text/comma-separated-values | csv       |        1028 | 2021-10-31 13:59:06.095023+00 | 2021-10-31 13:59:06.095049+00 | 2021-10-31 13:59:06.096332+00 |      5504 | audit.csv |
        #  1030 | kws/attachments/56_kws_immob_v1_0/audit_I04FqLP.csv | text/comma-separated-values | csv       |        1027 | 2021-10-31 13:57:45.163967+00 | 2021-10-31 13:57:45.163988+00 | 2021-10-31 13:57:45.165176+00 |      3765 | audit.csv |
        #  1029 | kws/attachments/56_kws_immob_v1_0/audit_b2gM6TU.csv | text/comma-separated-values | csv       |        1026 | 2021-10-31 13:57:44.779162+00 | 2021-10-31 13:57:44.779189+00 | 2021-10-31 13:57:44.780628+00 |      2124 | audit.csv |
        #  1028 | kws/attachments/56_kws_immob_v1_0/audit_gIzkoqE.csv | text/comma-separated-values | csv       |        1025 | 2021-10-31 13:57:44.401643+00 | 2021-10-31 13:57:44.401666+00 | 2021-10-31 13:57:44.402897+00 |      2558 | audit.csv |
        #  1027 | kws/attachments/56_kws_immob_v1_0/audit_sF5YYKv.csv | text/comma-separated-values | csv       |        1024 | 2021-10-31 13:57:44.027123+00 | 2021-10-31 13:57:44.027142+00 | 2021-10-31 13:57:44.028227+00 |      1948 | audit.csv |
        #  
        #  The column instance_id corresponds to the _id field of the exported submission

        return False

    def get_form_attachment(self, form_id):
        try:
            url = '%s/%s%s' % (self.server, self.form_rep, str(form_id))
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while registering a new profile')

    def publish_xls_form(self, xls_path, owner_username, api_token):
        try:
            url = '%s/%s' % (self.server, self.api_all_forms)
            itemsets = {'xls_file': open(xls_path, 'rb')}
            payload = {'owner': owner_username}
            xls_headers = {'Authorization': "Token %s" % api_token}
            print('Executing the url %s' % url)

            r = requests.get(url, files=itemsets, data=payload, headers=xls_headers)
            print(r.status_code)
            print(r.json())

            if r.status_code == 200:
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while publishing the XLS form')

    def publish_project_xls_form(self, xls_path, project_url, api_token):
        try:
            if settings.DEBUG: print('Publishing a form to a project')
            url = '%s/forms' % project_url
            itemsets = {'xls_file': open(xls_path, 'rb')}
            xls_headers = {'Authorization': "Token %s" % api_token}
            if settings.DEBUG: print('Executing the url %s' % url)

            r = requests.post(url, files=itemsets, headers=xls_headers)
            if r.status_code == 201:  # created
                return r.json()
            else:
                terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                raise Exception(r.text)
        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while publishing the XLS form')

    def get_user_token(self, username, user_password, api_token):
        '''
        Getting the user token is not a straightforward thing
        There is a hack to this by changing the user password, we get the new token
        So we 1st change the password to a temp password, then revert it back to the set password
        On reverting it back, we get the token
        '''
        try:
            url = '%s/%s' % (self.server, self.change_password % username)
            xls_headers = {'Authorization': "Token %s" % api_token}
            new_password = '%s%s' % (user_password, '123')
            payload = {'current_password': user_password, 'new_password': new_password}
            print('Executing dummy password url %s' % url)

            r = requests.post(url, data=payload, headers=xls_headers)
            if r.status_code != 200:
                raise Exception('There was an error while setting a new user password')

            payload = {'current_password': new_password, 'new_password': user_password}
            print('Reseting the real password -- %s' % url)
            r = requests.post(url, data=payload, headers=xls_headers)
            if r.status_code != 200:
                raise Exception('There was an error while re-setting user password')
            user_details = r.json()
            return user_details['access_token']

        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while getting the user token')

    def remove_user_from_project(self, project_url, username, role, api_token):
        # the username being deleted must be in lower case
        try:
            username = username.lower()
            url = '%s/share' % project_url
            xls_headers = {'Authorization': "Token %s" % api_token}
            payload = {'username': username, 'role': role, 'remove': True}
            print("Deleting the user '%s' via '%s'" % (username, url))

            r = requests.put(url, data=payload, headers=xls_headers)
            if r.status_code == 204: pass
            elif r.status_code == 404:
                if settings.DEBUG: terminal.tprint("The user '%s' was not found in the project. Perhaps the user was deleted" % username, 'info')
            else:
                print(r.json())
                raise Exception("There was an error while deleting the user '%s'" % username)

        except Exception as e:
            print(str(e))
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while deleting a user from the project')

    def delete_project(self, project_url, api_token):
        try:
            url = project_url
            xls_headers = {'Authorization': "Token %s" % api_token}
            print("Deleting the project '%s'" % url)

            r = requests.delete(url, headers=xls_headers)
            
            if r.status_code == 204: pass
            elif r.status_code == 404:
                if settings.DEBUG: terminal.tprint("The project '%s' was not found. Perhaps it was deleted" % url, 'info')
            else:
                print(r.json())
                raise Exception("There was an error while deleting the project '%s'" % url)

        except Exception as e:
            if settings.DEBUG: print((traceback.format_exc()))
            sentry.captureException()
            raise Exception('There was an error while deleting a user from the project')



