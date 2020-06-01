import requests
import traceback

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
        self.media = 'api/v1/media'
        self.metadata_uri = 'api/v1/metadata'

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
            url = '%s%s' % (self.server, 'api/v1/profiles')
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
            print((traceback.format_exc()))
            terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while registering a new profile')

    def upload_itemsets_csv(self, file_name):
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

                # we are updating this form
                # we might need to delete the existing metadata
                # print("\n\nupdating %s itemsets.csv" % form['id_string'])
                # continue

                # check if we have metadata
                meta_url = '%s%s?xform=%s' % (self.server, self.metadata_uri, form['formid'])
                meta_r = requests.get(meta_url, headers=self.headers)
                # print("Fetching meta response code %s" % meta_r.status_code)
                if meta_r.status_code != 200:
                    terminal.tprint("Response %d: %s" % (meta_r.status_code, meta_r.text), 'fail')
                    raise Exception(meta_r.text)

                for form_meta in meta_r.json():
                    if form_meta['data_value'] == 'itemsets.csv':
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
                payload = {'data_type': 'media', 'data_value': 'itemsets.csv', 'xform': form['formid']}

                r = requests.post(url, files=itemsets, data=payload, headers=self.headers)
                # print("Media update response code %s " % r.status_code)
                
                if r.status_code != 201:
                    # terminal.tprint("Response %d: %s" % (r.status_code, r.text), 'fail')
                    raise Exception(r.text)
        except ConnectionError as e:
            raise Exception("%s\n%s" % ("We can't establish a connection to the onadata server", str(e)))
        except Exception as e:
            raise Exception(e)
