import smtplib
import os
import sys
import re
import shutil
import requests
import boto3
import json
import sentry_sdk

from botocore.exceptions import ClientError
from django.conf import settings
from decimal import Decimal


# import django-rq if we are using queues
try:
    import django_rq
except Exception:
    pass

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from jinja2 import Template, FileSystemLoader
from jinja2.environment import Environment

try:
    from vendor.terminal_output import Terminal
except Exception:
    try:
        from toolkit.terminal_output import Terminal
    except Exception:
        from .terminal_output import Terminal

terminal = Terminal()
sentry_sdk.init(settings.SENTRY_DSN)


class Emails():
    def __init__(self):
        terminal.tprint("Initializing the Email class", 'ok')


    def initiate_send_email(self, email_settings):
        try:
            # print(email_settings)
            print(settings.TEMPLATES[0]['DIRS'][0])
            self.env = Environment()
            self.env.loader = FileSystemLoader(settings.TEMPLATES[0]['DIRS'][0])

            template = self.env.get_template(email_settings['template'])
            email_settings['site_name'] = settings.SITE_NAME
            email_html = template.render(email_settings)
            cc = email_settings['cc'] if 'cc' in email_settings else None
            use_queue = email_settings['use_queue'] if 'use_queue' in email_settings else False

            Emails.send_email(email_settings['recipient_email'], email_settings['sender_email'], cc, email_settings['subject'], email_html, use_queue)
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            # sentry.captureException()
            raise 

    def send_email(to, sender, cc, subject=None, body=None, add_to_queue=False):
        ''' sends email using a Jinja HTML template '''
        # convert TO into list if string
        if type(to) is not list:
            to = to.split()

        to_list = to
        msg = MIMEMultipart('alternative')
        msg['From'] = settings.SITE_NAME
        msg['Subject'] = subject
        msg['To'] = ','.join(to)
        if cc is not None:
            msg['Cc'] = ','.join(cc)
            to_list = to_list + cc

        to_list = [_f for _f in to_list if _f]             # remove null emails

        msg.attach(MIMEText('Alternative text', 'plain'))
        msg.attach(MIMEText(body, 'html'))
        try:
            terminal.tprint('setting up the SMTP con....', 'debug')
            if add_to_queue == True:
                django_rq.enqueue(queue_email, to_list, msg)
            else:
                server = smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT)
                server.starttls()
                server.login(settings.SENDER_EMAIL, settings.SENDER_PASSWORD)
                server.sendmail(settings.SITE_NAME, to_list, msg.as_string())
                server.quit()
        except Exception as e:
            terminal.tprint('Error sending email -- %s' % str(e), 'error')
            raise Exception('Error sending email -- %s' % str(e))
            

def queue_email(to_list, msg):
    try:
        server = smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT)
        server.starttls()
        server.login(settings.SENDER_EMAIL, settings.SENDER_PASSWORD)
        server.sendmail(settings.SITE_NAME, to_list, msg.as_string())
        server.quit()
    except Exception:
        raise

    def render_template(self, template, params, **kwargs):
        ''' renders a Jinja template into HTML '''
        # check if template exists
        template_path = '%s/%s' % (settings.TEMPLATES[0]['DIRS'][0], template)
        if not os.path.exists(template_path):
            print(('No template file present: %s' % template_path))
            sys.exit()

        import jinja2
        return jinja2.load_template(template)
        
        templateLoader = jinja2.FileSystemLoader(searchpath="./")
        templateEnv = jinja2.Environment(loader=templateLoader)
        templ = templateEnv.get_template(template_path)
        return templ.render(**kwargs)

        env = Environment(loader=PackageLoader('poultry', 'templates'))
        template = env.get_template(template)
        return template.render(params)


class ProgressBar():
    def __init__(self):
        # silence is golden
        print('')

    # The MIT License (MIT)
    # Copyright (c) 2016 Vladimir Ignatev
    #
    # Permission is hereby granted, free of charge, to any person obtaining
    # a copy of this software and associated documentation files (the "Software"),
    # to deal in the Software without restriction, including without limitation
    # the rights to use, copy, modify, merge, publish, distribute, sublicense,
    # and/or sell copies of the Software, and to permit persons to whom the Software
    # is furnished to do so, subject to the following conditions:
    #
    # The above copyright notice and this permission notice shall be included
    # in all copies or substantial portions of the Software.
    #
    # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
    # INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
    # PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
    # FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
    # OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE
    # OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
    def progress(self, count, total, status=''):
        bar_len = 60
        filled_len = int(round(bar_len * count / float(total)))

        percents = round(100.0 * count / float(total), 1)
        bar = '=' * filled_len + '-' * (bar_len - filled_len)

        sys.stdout.write('\r[%s] %s%s ...%s\r' % (bar, percents, '%', status))
        sys.stdout.flush()  # As suggested by Rom Ruben (see: http://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console/27871113#comment50529068_27871113)


class SQLManipulations():
    def __init__(self):
        # silence is golden
        terminal.tprint("Initializing SQL manipulations", 'ok')

    def dictfetchall(cursor):
        "Return all rows from a cursor as a dict"
        columns = [col[0] for col in cursor.description]
        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        
        return json.JSONEncoder.default(self, obj)

def validate_phone_number(phone_no):
    # given any phone number, validates it and returns a valid number else returns None
    phone_number = re.findall('^\(?(?:\+?254|0)((?:7|1)\)?(?:[ -]?[0-9]){2}\)?(?:[ -]?[0-9]){6})$', phone_no)
    if len(phone_number) == 0:
        return None
    else:
        return '+254%s' % phone_number[0]

def download_image_from_url(settings_):
    # given a URL, downloads the image and saves it to the defined path or AWS
    try:
        # retrieve the image name to use
        path_ = os.path.split(settings_['img_url'])
        local_path = '%s/%s' % (settings_['path'], path_[1])
        response = requests.get(settings_['img_url'], stream=True)
        
        if response.status_code == 200:
            with open(local_path, 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)

            if 'upload_2_s3' in settings_ and settings_['upload_2_s3']:
                # push it to s3
                client = boto3.client('s3', region_name=settings.AWS_S3_REGION_NAME)
                img_name = path_[1]
                img_path = '%s/%s' % (settings_['s3_path'], path_[1])
                s3_response = client.upload_file(path_[1], settings.AWS_STORAGE_BUCKET_NAME, img_path, ExtraArgs={'ACL':'public-read'})

            del response

            if 'del_local_file' in settings_ and settings_['del_local_file']:
                os.remove(local_path)
            return (img_name, img_path, None)
        else:
            del response
            return (None, None, "There was an error while downloading the image '%s' from the server" % settings_['img_url'])
            
    except ClientError as e:
        if settings.DEBUG: terminal.tprint(str(e), 'debug')
        raise 
    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        raise

def send_sentry_message(message='Test message', err_level='info', extra_data=None, tags=None, user_data=None):
    with sentry_sdk.push_scope() as scope:
        if tags:
            for tag in tags:
                scope.set_tag(tag['tag'], tag['value'])
            
        if user_data:
            for u in user_data:
                scope.user = {u['tag'] : u['value']}

        if extra_data:
            for ed in extra_data:
                scope.set_extra(ed['tag'], ed['value'])

        sentry_sdk.capture_message(message, err_level)

def process_geopoint_node_v1(column, gps_string):
    """
    v1: Simplifies the processing of the nodes
    """
    # split the data by a space as expected from odk
    geo = gps_string.split()

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
    if re.search('accuracy', column):
        # we have altitude 
        return geo[3]

    raise Exception('Unknown Destination Column: Encountered a GPS data field (%s), but I cant seem to deduce which type(latitude, longitude, altitude) the current column (%s) is.' % (gps_string, column))
