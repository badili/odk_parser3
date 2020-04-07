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
