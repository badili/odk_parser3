"""The main processing unit for all notifications

This module contains the core functions in the sms queue
"""
import csv
import re
import datetime
import uuid
import pytz
import random
import json

from datetime import date
from django.conf import settings
from django.contrib.auth.hashers import make_password
from django.db import transaction, connection
from django.db.models import Q, Sum, Count, IntegerField, Min, Max, Avg
from django.forms.models import model_to_dict
from django.utils import timezone
from raven import Client

from jinja2 import Template, FileSystemLoader
from jinja2.environment import Environment

try:
    if re.match('.+LivHealth', settings.SITE_NAME):
        # try importing stuff from LivHealth
        from .terminal_output import Terminal
        from .models import SMSQueue, MessageTemplates, Recipients, Campaign, SubCounty, Ward, Village
        from .serializers import RecepientSerializer
        from .odk_forms import OdkForms
        from .odk_choices_parser import ImportODKChoices
    elif settings.SITE_NAME == 'Pazuri Records':
        # try importing stuff from PazuriPoultry
        from vendor.terminal_output import Terminal
        from poultry.models import SMSQueue, MessageTemplates, Campaign, Personnel, Farm, SubscriptionPayment, Batch, Production, EventsSchedule, EventsList, OtherEvents, Farm, IncomeExpense, PERSONNEL_DESIGNATION_CHOICES
        from poultry.common_tasks import Emails
    elif settings.SITE_NAME == 'BoxGirls M&E System':
        from vendor.terminal_output import Terminal
        from me_app.common_tasks import Emails
    elif settings.SITE_NAME == 'Church Register':
        from .terminal_output import Terminal
        from .common_tasks import Emails
    elif settings.SITE_NAME == 'Co-Infection Data Hub':
        from coinfection.terminal_output import Terminal
        from coinfection.common_tasks import Emails
    elif settings.SITE_NAME == 'AAGRIS':
        from aagris.terminal_output import Terminal
        from aagris.common_tasks import Emails
    elif settings.SITE_NAME == 'FLAIR Project':
        from toolkit.terminal_output import Terminal
        from toolkit.common_tasks import Emails
        
    else:
        from toolkit.terminal_output import Terminal
        from toolkit.common_tasks import Emails
except Exception as e:
    raise


terminal = Terminal()
sentry = Client(settings.SENTRY_DSN)

current_tz = pytz.timezone(settings.TIMEZONE)
timezone.activate(current_tz)


class Notification():
    def __init__(self):
        self.time_formats = ['now', 'today', 'tomorrow', 'yesterday']
        self.at_ok_sending_status_codes = [100, 101, 102]

        try:
            # email environment
            self.email_obj = Emails()
        except:
            # we might not be needing the email module as of now
            pass
        self.env = Environment()
        self.env.loader = FileSystemLoader(settings.TEMPLATES[0]['DIRS'][0])

        # testing messages
        if hasattr(settings, 'TESTING_PREFIX'):
            self.testing_numbers = ['%s%s' % (settings.TESTING_PREFIX, x) for x in range(settings.TESTING_PHONE_NUMBERS_START, settings.TESTING_PHONE_NUMBERS_END)]
        else:
            self.testing_numbers = []

    def process_test_data(self, input_file):
        """Given an input file, imports the data to the DB

        Allows initialization of base data to the database.
        """
        terminal.tprint('Processing the file %s...' % input_file, 'info')

        try:
            transaction.set_autocommit(False)
            with open(input_file, 'rt') as in_file:
                test_data = csv.DictReader(in_file, delimiter=',', quotechar='"')
                for row in test_data:
                    self.process_test_message(row)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            sentry.captureException()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')

        terminal.tprint("The input file '%s' with test data has been processed successfully..." % input_file, 'info')

    def process_test_message(self, mssg):
        # if the message to be sent is empty, just ignore the line
        if mssg['message'].strip() == '':
            terminal.tprint('We have an empty message, nothing to do here...', 'info')
            return

        # generate the message uuid
        mssg_uuid = uuid.uuid5(uuid.NAMESPACE_X500, mssg['message'])

        # check if we need to add a campaign
        if mssg['campaign'] != '':
            try:
                cur_campaign = Campaign.objects.filter(campaign_name=mssg['campaign']).get()
            except Campaign.DoesNotExist:
                cur_campaign = self.save_auto_campaign(mssg['campaign'])
        else:
            cur_campaign = None

        # check if we have a sending time
        cur_time = timezone.localtime(timezone.now())
        print(mssg['sending_time'])
        if mssg['sending_time'] != '':
            mssg_sending_time = mssg['sending_time'].strip()
            # check if the data specified is correct, else throw an error
            if mssg_sending_time in self.time_formats:
                if mssg_sending_time == 'now' or mssg_sending_time == 'today':
                    schedule_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
                elif mssg_sending_time == 'tomorrow':
                    schedule_time = (cur_time + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
                elif mssg_sending_time == 'yesterday':
                    schedule_time = (cur_time + datetime.timedelta(days=-1)).strftime('%Y-%m-%d %H:%M:%S')
            else:
                try:
                    schedule_time = timezone.datetime.strptime(mssg['sending_time'], '%Y-%m-%d %H:%M:%S')
                except ValueError as e:
                    terminal.tprint(str(e), 'fail')
                    raise ValueError("Incorrect sending time specified. The sending time can only be '%s' or a valid date time string eg. 2019-09-23 14:41:00" % ', '.join(self.time_formats))
        else:
            schedule_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')

        # check if the message is already added to the template
        try:
            msg_template = MessageTemplates.objects.filter(uuid=mssg_uuid, campaign=cur_campaign).get()
        except MessageTemplates.DoesNotExist:
            msg_template = self.add_message_template(mssg['message'], mssg_uuid, cur_campaign)

        # split the messages into parts if need be
        messages = self.check_message_length(mssg['message'].strip())

        # split the recipients of the message and add to the queues
        for rec in mssg['recepient_nos'].split(','):
            rec = rec.strip()
            if len(rec) == 0:
                continue

            if re.search('^\+\d+$', rec) is None:
                err_mssg = 'The recipients phone number must begin with a plus(+) sign and contain only integers'
                terminal.tprint(err_mssg, 'fail')
                raise Exception(err_mssg)
            try:
                recipient = Recipients.objects.filter(recipient_no=rec).get()

                # everything is now really good... so lets add this to the queue
                # Django saves all the dates and times to the database in the UTC timezone
                # loop through the messages and add them to the queue
                for cur_mssg in messages:
                    queue_item = SMSQueue(
                        template=msg_template,
                        message=cur_mssg,
                        recipient=recipient,
                        recipient_no=rec,
                        msg_status='SCHEDULED',
                        schedule_time=schedule_time
                    )
                    queue_item.full_clean()
                    queue_item.save()
            except Recipients.DoesNotExist:
                recipient = self.add_recipient(rec)
            except Exception as e:
                if settings.DEBUG: terminal.tprint(str(e), 'fail')
                sentry.captureException()
                raise Exception(str(e))

    def check_message_length(self, message):
        """

        Given a message, check if it is within the acceptable message length, if not, split it into parts

        Args:
            message (string): The message to check its length

        Returns
            An array of strings with the messages. In the array the messages are ordered in order that they should be sent
        """
        if len(message) > settings.SMS_MAX_LENGTH:
            # using range determine the indexes of the string to slice
            # iterate through the indexes and get the subset of the message
            # append the subsets to an array
            # return the array
            messages = []
            mssg_parts = range(0, len(message), settings.SMS_MAX_LENGTH)
            for i, j in zip(mssg_parts, range(len(mssg_parts))):
                messages.append('%s %d/%d' % (message[i:i + settings.SMS_MAX_LENGTH], j + 1, len(mssg_parts)))

            # return the messages
            return messages
        else:
            return [message]

    def save_auto_campaign(self, campaign_name):
        """Save a campaign since it does not exist

        Returns:
            The campaign object which has been created
        """
        try:
            cur_campaign = Campaign(
                campaign_name=campaign_name
            )
            cur_campaign.full_clean()
            cur_campaign.save()

            return cur_campaign
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise

    def add_recipient(self, recipient_no, first_name=None, other_names=None):
        """Adds a recipient to the database since they don't exist

        The recipient does not exist, so lets add to them to the database

        Args:
            recipient_no (string): the phone number of the recipient
            first_name (string | optional): the first name
            other_names (string | optional): the other names of the reepient

        Returns:
            Returns the created recipient
        """

        try:
            # if the names haven't been provided, using a faker, populate placeholder names
            if first_name is None:
                fake_p = Faker()
                first_name = fake_p.name().split(' ')[0]
            if other_names is None:
                other_names = fake_p.name().split(' ')[1]

            recipient = Recipients(
                recipient_no=recipient_no,
                first_name=first_name,
                other_names=other_names
            )
            recipient.full_clean()
            recipient.save()
            return recipient
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def add_message_template(self, template, uuid, campaign):
        """Adds a message template to the database

        Adds a message template to the database since it does not exist

        Args:
            template (string): The message template to add to the database
            campaign (Campaign or None): The campaign to associate the message to

        Returns:
            Returns the saved campaign
        """

        try:
            mssg_template = MessageTemplates(template=template, uuid=uuid)
            if campaign is not None:
                mssg_template.campaign = campaign

            mssg_template.full_clean()
            mssg_template.save()

            return mssg_template
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def process_scheduled_sms(self, provider):
        """Processes the scheduled SMS and puts them in the sending queue

        Fetches all the scheduled SMSes from the databases and adds them to a sending queue
        """

        cur_time = timezone.localtime(timezone.now())
        cur_time_str = cur_time.strftime('%Y-%m-%d %H:%M:%S')
        use_provider = provider
        try:
            gateway_ids = list(settings.SMS_GATEWAYS['gateways'].keys())

            # get the statuses to use to fetch the sms to process
            statuses_to_use = ['SCHEDULED']
            statuses_keys = list(settings.AT_STATUS_CODES.keys())
            for status_code in statuses_keys:
                if status_code not in self.at_ok_sending_status_codes:
                    statuses_to_use.append(settings.AT_STATUS_CODES[status_code])

            if provider not in gateway_ids and provider is not None:
                raise Exception("'%s' is not configured as a gateway provider. Select from '%s'" % (provider, ', '.join(gateway_ids)))

            if provider is None:
                use_provider = random.choice(list(settings.SMS_GATEWAYS['gateways'].keys()))

            # fetch the sms whose sending schedule time has passed
            sms2send = SMSQueue.objects.filter(schedule_time__lte=cur_time_str, msg_status__in=statuses_to_use).order_by('id').all()
            for sched_sms in sms2send:
                if sched_sms.recipient_no in self.testing_numbers:
                    # we have a testing message, don't send it
                    continue
                # print('%s: %s - %s' % (sched_sms.id, sched_sms.schedule_time, sched_sms.recipient_no))
                print('Seconds Diff\nCur Time = %s, Sched Time = %s\n````````````\n%.1f -- %d\n--\n' % (cur_time_str, sched_sms.schedule_time, (cur_time - sched_sms.schedule_time).total_seconds(), settings.MESSAGE_VALIDITY * 60 * 60))
                if (cur_time - sched_sms.schedule_time).total_seconds() > settings.MESSAGE_VALIDITY * 60 * 60:
                    if settings.DEBUG: print('The message is expired...')
                    sentry.captureMessage("Expired message to %s: '%s'" % (sched_sms.recipient_no, sched_sms.message), level='warning', extra={'cur_time': cur_time_str, 'scheduled_time': sched_sms.schedule_time.strftime('%Y-%m-%d %H:%M:%S'), 'message_validity': '%d Sec' % settings.MESSAGE_VALIDITY * 60 * 60})
                    sched_sms.msg_status = 'EXPIRED'
                    sched_sms.full_clean()
                    sched_sms.save()
                    continue

                # if we have testing messages, don't send them
                
                if use_provider == 'at':
                    terminal.tprint('Sending the SMS via AT...', 'info')
                    self.send_via_at(sched_sms)
                elif use_provider == 'nexmo':
                    terminal.tprint('Sending the SMS via Nexmo...', 'info')
                    self.send_via_nexmo(sched_sms)
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()

        # def queue_via_at(self, mssg):

    def configure_at(self):
        """Configures and initializes AfricasTalking as an SMS gateway provider

        Using the settings provided in the settings file, configures and initializes AT as an SMS gateway
        """
        import africastalking

        username = settings.SMS_GATEWAYS['gateways']['at']['USERNAME']
        api_key = settings.SMS_GATEWAYS['gateways']['at']['KEY']
        # print("AT: Using the creds: %s - %s" % (username, api_key))
        africastalking.initialize(username, api_key)
        self.at_sms = africastalking.SMS

    def send_via_at(self, mssg):
        """Submits a message to be sent via the AT gateway

        Args:
            The message object as JSON to be sent
        """
        try:
            # queue the message to be sent via africastalking. Once queued, update the database with the queue status
            cur_time = timezone.localtime(timezone.now())
            cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
            # lets send the messages synchronously... should be changed to async
            # How does AT identify a message when a callback is given
            this_resp = self.at_sms.send(mssg.message, [mssg.recipient_no], settings.AT_SENDER_ID, False)

            # print(this_resp)
            if len(this_resp['SMSMessageData']['Recipients']) == 0:
                # print(mssg)
                sentry.captureMessage("Message not sent.", level='info', extra={'at_response': this_resp, 'message': mssg.message, 'recipient': mssg.recipient_no, 'sender_id': settings.AT_SENDER_ID})
                # raise Exception(this_resp['SMSMessageData']['Message'])
            elif this_resp['SMSMessageData']['Recipients'][0]['statusCode'] in self.at_ok_sending_status_codes:
                # if the message is processed well, add the results to the db
                mssg.in_queue = 0
                mssg.queue_time = cur_time
                mssg.provider_id = this_resp['SMSMessageData']['Recipients'][0]['messageId']

                mssg.msg_status = settings.AT_STATUS_CODES[this_resp['SMSMessageData']['Recipients'][0]['statusCode']]
                mssg.full_clean()
                mssg.save()
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def process_at_report(self, request):
        """Process a notification from AT gateway

        """
        try:
            # get the smsqueue and update its status
            delivery_type = request.GET.get('type')
            if delivery_type == 'delivery':
                self.process_at_delivery_report(request)
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def process_at_delivery_report(self, request):
        """Process a delivery notification from africastalking

        """
        try:
            # get the smsqueue and update its status
            sms_id = request.POST.get('id')
            sms_status = request.POST.get('status')
            queue_instance = SMSQueue.objects.filter(provider_id=sms_id).get()

            if sms_status in settings.AT_FINAL_DELIVERY_STATUS:
                # the sms has a final delivery status... so lets add this to the database
                queue_instance.msg_status = sms_status
                cur_time = timezone.localtime(timezone.now())
                cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
                queue_instance.delivery_time = cur_time

                queue_instance.full_clean()
                queue_instance.save()
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def configure_nexmo(self):
        """Configure the NEXMO SMS gateway

        """
        import nexmo

        key = settings.SMS_GATEWAYS['gateways']['nexmo']['KEY']
        secret = settings.SMS_GATEWAYS['gateways']['nexmo']['SECRET']
        # print("NEXMO: Using the creds: %s - %s" % (key, secret))
        self.nexmo = nexmo.Client(key=key, secret=secret)

    def send_via_nexmo(self, mssg):
        """Sends a message using the configured NEXMO SMS gateway

        Args:
            mssg (json); The message to be sent
        """
        try:
            # queue the message to be sent via africastalking. Once queued, update the database with the queue status
            cur_time = timezone.localtime(timezone.now())
            cur_time = cur_time.strftime('%Y-%m-%d %H:%M:%S')
            # for nexmo we need to strip out the preceeding +. All our numbers have a preceeding +
            recipient = mssg.recipient_no.split('+')[1]
            this_resp = self.nexmo.send_message({
                'from': 'Wangoru Kihara',
                'to': recipient,
                'text': mssg.message,
                'ttl': settings.SMS_VALIDITY * 60 * 60          # specify a TTL since NEXMO allows this
            })

            print(this_resp)
            if this_resp["messages"][0]["status"] == "0":
                mssg.in_queue = 1
                mssg.queue_time = cur_time
                mssg.msg_status = settings.NEXMO_STATUS_CODES[int(this_resp["messages"][0]["status"])]
                mssg.provider_id = this_resp['messages'][0]['message-id']

                mssg.full_clean()
                mssg.save()
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def process_nexmo_report(self, request):
        """Process a notification from NEXMO gateway

        """
        try:
            # get the smsqueue and update its status
            delivery_type = request.GET.get('type')
            if delivery_type == 'delivery':
                self.process_nexmo_delivery_report(request)
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def process_nexmo_delivery_report(self, request):
        """Process a delivery notification from africastalking

        """
        try:
            # get the smsqueue and update its status
            sms_id = request.POST.get('messageId')
            sms_status = request.POST.get('status')

            queue_instance = SMSQueue.objects.filter(provider_id=sms_id).get()

            if sms_status in settings.NEXMO_FINAL_DELIVERY_STATUS:
                # the sms has a final delivery status... so lets add this to the database
                queue_instance.msg_status = settings.NEXMO_DELIVERY_CODES[sms_status]
                mssg_time = request.POST.get('message-timestamp')
                delivery_time = timezone.datetime.strptime(mssg_time, '%Y-%m-%d %H:%M:%S')
                queue_instance.delivery_time = delivery_time

                queue_instance.full_clean()
                queue_instance.save()
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def get_notification_settings(self):
        """Get the settings defined for the different notifications
        """
        try:
            data_set = {
                'campaigns': Campaign.objects.all(),
                'templates': MessageTemplates.objects.order_by('template_name').all(),
                'recipients': Recipients.objects.select_related('village__ward__sub_county').select_related('ward').select_related('sub_county').order_by('first_name').all(),
                'recipient_types': Recipients.objects.order_by().values('designation').distinct(),
                'sub_counties': SubCounty.objects.order_by('sub_county_name').all(),
                'wards': [model_to_dict(rec) for rec in Ward.objects.order_by('ward_name').all()],
                'villages': [model_to_dict(rec) for rec in Village.objects.order_by('village_name').all()],
            }

            return data_set
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def save_campaign(self, request):
        try:
            # get the campaign details and add them to the database
            campaign_name = request.POST.get('campaign-name')
            recipients = request.POST.getlist('recipients[]')
            scheduled = request.POST.get('schedule-day')

            transaction.set_autocommit(False)
            if request.POST.get('object_id'):
                # we are editing a campaign
                campaign = Campaign.objects.filter(id=request.POST.get('object_id')).get()
                campaign.campaign_name=campaign_name
                campaign.recipients=','.join(recipients)
                campaign.schedule_time=scheduled
            else:
                campaign = Campaign(
                    campaign_name=campaign_name,
                    recipients=','.join(recipients),
                    schedule_time=scheduled
                )
            campaign.full_clean()
            campaign.save()
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def save_template(self, request):
        try:
            # get the campaign details and add them to the database
            template_name = request.POST.get('template-name')
            template_type = request.POST.get('message-type')
            template_message = request.POST.get('template-message')
            campaign_id = request.POST.get('campaign')
            sending_time = request.POST.get('sending-time') or None
            mssg_uuid = uuid.uuid5(uuid.NAMESPACE_X500, template_message)

            # get the campaign names for this template
            campaign = Campaign.objects.filter(id=campaign_id).get()

            transaction.set_autocommit(False)
            if request.POST.get('object_id'):
                template = MessageTemplates.objects.filter(id=request.POST.get('object_id')).get()
                template.template_name=template_name
                template.template_type=template_type
                template.campaign=campaign
                template.template=template_message
                template.uuid=mssg_uuid
            else:
                template = MessageTemplates(
                    template_name=template_name,
                    template_type=template_type,
                    campaign=campaign,
                    template=template_message,
                    uuid=mssg_uuid
                )

            if sending_time is not None:
                template.sending_time=sending_time
            template.full_clean()
            template.save()
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def save_recipient(self, request):
        try:
            # get the campaign details and add them to the database
            salutation = request.POST.get('salutation')
            first_name = request.POST.get('first-name').strip()
            other_names = request.POST.get('other-names').strip()
            designation = request.POST.get('designation')
            email = request.POST.get('email').strip()
            cell_no = request.POST.get('cell_no').strip()
            alternative_cell_no = request.POST.get('alternative_cell_no').strip() if request.POST.get('alternative_cell_no') != '' else None
            sub_county_id = request.POST.get('sub-county')
            ward_id = request.POST.get('ward').strip()
            village_id = request.POST.get('village').strip()

            # get the campaign names for this template
            if sub_county_id == '-1' or sub_county_id == '':
                sub_county = None
                ward = None
                village = None
            else:
                sub_county = SubCounty.objects.filter(id=sub_county_id).get()
                if ward_id != '' and ward_id.isnumeric() is False:
                    # we have a new ward...
                    odk_choices_parser = ImportODKChoices()
                    new_ward = {
                        'label': ward_id,
                        'name': ward_id.replace("'.- ", '').lower(),
                        'ward_subcounty': sub_county.nick_name
                    }
                    ward = odk_choices_parser.process_ward(new_ward)
                else:
                    ward = Ward.objects.filter(id=ward_id).get() if ward_id != '' else None
                
                if village_id != '' and village_id.isnumeric() is False:
                    # we have a new village...
                    odk_choices_parser = ImportODKChoices()
                    new_village = {
                        'label': village_id,
                        'name': village_id.replace("'.- ", '').lower(),
                        'village_ward': ward.nick_name
                    }
                    village = odk_choices_parser.process_village(new_village)
                else:
                    village = Village.objects.filter(id=village_id).get() if village_id != '' else None

            transaction.set_autocommit(False)
            if request.POST.get('object_id'):
                recipient = Recipients.objects.filter(id=request.POST.get('object_id')).get()
                recipient.salutation = salutation
                recipient.first_name = first_name
                recipient.other_names = other_names
                recipient.designation = designation
                recipient.cell_no = cell_no
                recipient.alternative_cell_no = alternative_cell_no
                recipient.recipient_email = email
                recipient.village = village
                recipient.ward = ward
                recipient.sub_county = sub_county
            else:
                # fabricate a nick_name for the recipient
                nick_name = '%s_%s' % (first_name.replace("'.- ", '').lower(), other_names.replace("'.- ", '').lower())
                recipient = Recipients(
                    salutation=salutation,
                    first_name=first_name,
                    other_names=other_names,
                    designation=designation,
                    cell_no=cell_no,
                    alternative_cell_no=alternative_cell_no,
                    recipient_email=email,
                    nick_name=nick_name,
                    village=village,
                    ward=ward,
                    sub_county=sub_county
                )
            recipient.full_clean()
            recipient.save()
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def periodic_processing(self, provider):
        """Run the periodic procesing script.

        This will process the received data and queue messages to be sent if need be
        and also sends out the queued notifications via the respective channels
        """

        # process the sms to be sent
        self.process_notifications_data()

        # submit the queued SMS to the provider
        self.process_scheduled_sms(provider)

    def process_notifications_data(self):
        # if need be, crunch the data for feedback to the users
        # Notifications are scheduled on the specified day at settings.SENDING_SPLIT

        cur_time = timezone.localtime(timezone.now())
        parts = settings.SENDING_TIME.split(':')
        sending_time = cur_time.replace(hour=int(parts[0]), minute=int(parts[1]), second=int(parts[2]))

        split_seconds = (cur_time - sending_time).total_seconds()
        print('\n%s: Splits\n````````````\n%.1f -- %.1f\n--\n' % (cur_time.strftime('%A %d-%m %H:%M:%S'), split_seconds, settings.SENDING_SPLIT))

        # loop through the campaigns and determine the one that needs processing
        campaigns = Campaign.objects.all()
        odk_form = OdkForms()
        i = 0
        for campaign in campaigns:
            if cur_time.strftime('%A') == campaign.schedule_time:
                # print('\n%s campaign should be ran today...' % campaign.campaign_name)
                # the campaign should ran today
                if split_seconds > -1 and split_seconds < settings.SENDING_SPLIT:
                    # get the templates assigned to this campaign that needs to be processed
                    templates = MessageTemplates.objects.filter(campaign_id=campaign.id).all()
                    for template in templates:
                        # print(template.template_name)
                        if template.template_name == 'Management Weekly Report' or template.template_name == 'LivHealth Admin Weekly Feedback':
                            stats = self.management_weekly_report(odk_form, odk_form.sub_counties)

                        # get the users in this campaign
                        user_groups = campaign.recipients.split(',')
                        recipients = Recipients.objects.filter(designation__in=user_groups)
                        sub_counties_stats = {}
                        for recipient in recipients:
                            if template.template_name == 'SCVO Weekly Reminder':
                                # recipient_name
                                message = template.template % (recipient.first_name)
                            elif template.template_name == 'SCVO Weekly Report':
                                if recipient.sub_county.nick_name not in sub_counties_stats:
                                    sc_stats = self.management_weekly_report(odk_form, [recipient.sub_county.nick_name, ''])
                                    sub_county_name = str(odk_form.get_value_from_dictionary(recipient.sub_county.nick_name))
                                    sub_counties_stats[recipient.sub_county.nick_name] = {'stats': sc_stats, 'sub_county_name': sub_county_name}
                                else:
                                    sc_stats = sub_counties_stats[recipient.sub_county.nick_name]['stats']
                                    sub_county_name = sub_counties_stats[recipient.sub_county.nick_name]['sub_county_name']

                                message = template.template % tuple([recipient.first_name] + [sub_county_name] + [sc_stats[0], sc_stats[2]])
                            elif template.template_name == 'Management Weekly Report' or template.template_name == 'LivHealth Admin Weekly Feedback':
                                # name, # syndromic reports, # abbatoirs, # ND1 reports, # agrovet reports from the last week
                                message = template.template % tuple([recipient.first_name] + stats)

                            if recipient.cell_no or recipient.alternative_cell_no:
                                # print('\n%s: %s' % (template.template_name, message))
                                odk_form.schedule_notification(template, recipient, message)
                                i = i + 1

                            # one recipent per template if debug is True
                            # if settings.DEBUG:
                            #     break
                    # print(sub_counties_stats)

        print('\nSent %d messages\n' % i)

    def management_weekly_report(self, odk_form, sub_counties):
        # get the number of reports received from the previous week, monday to sunday
        today = timezone.datetime.today()
        end_date = today + datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=7)

        stats = odk_form.dash_stats(start_date, end_date, sub_counties, odk_form.all_species)

        # get the number of ND1 reports
        nd_reporting_q = """
            SELECT count(*)
            FROM nd_details as a INNER JOIN nd_reports as b on a.nd_report_id=b.id
            WHERE nd_date_reported > '%s' AND nd_date_reported < '%s' AND sub_county IN %s
        """ % (str(start_date), str(end_date), tuple(sub_counties))
        # print(nd_reporting_q)

        sh_reporting_q = """
            SELECT count(*)
            FROM sh_reports as a
            WHERE report_date > '%s' AND report_date < '%s'
        """ % (str(start_date), str(end_date))
        # print(sh_reporting_q)

        ag_reporting_q = """
            SELECT count(*)
            FROM ag_detail as a INNER JOIN ag_reports as b on a.ag_report_id=b.id
            WHERE report_date > '%s' AND report_date < '%s'
        """ % (str(start_date), str(end_date))
        # print(ag_reporting_q)

        with connection.cursor() as cursor:
            cursor.execute(nd_reporting_q)
            nd_reporting = cursor.fetchall()

            cursor.execute(sh_reporting_q)
            sh_reporting = cursor.fetchall()

            cursor.execute(ag_reporting_q)
            ag_reporting = cursor.fetchall()

        return [stats['total_submissions'], sh_reporting[0][0], nd_reporting[0][0], ag_reporting[0][0]]

    def get_sent_notifications(self):
        # get the list of sent notifications
        notifications = SMSQueue.objects.select_related('recipient').select_related('template').order_by('-schedule_time').all()

        return {'notifications': notifications}

    def send_email(self, email_settings):
        try:
            # print(email_settings)
            template = self.env.get_template(email_settings['template'])
            email_settings['site_name'] = settings.SITE_NAME
            email_html = template.render(email_settings)
            cc = email_settings['cc'] if 'cc' in email_settings else None
            use_queue = email_settings['use_queue'] if 'use_queue' in email_settings else False

            Emails.send_email(email_settings['recipient_email'], email_settings['sender_email'], cc, email_settings['subject'], email_html, use_queue)
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))


class PazuriNotification():
    def __init__(self, cur_user_email=None):
        self.time_formats = ['now', 'today', 'tomorrow', 'yesterday']
        self.at_ok_sending_status_codes = [100, 101, 102]

        try:
            if cur_user_email is not None:
                # save the cur user object
                self.cur_user = Personnel.objects.filter(email=cur_user_email).get()

                # get the farm of this user
                self.cur_farm_id = self.cur_user.farm_id
                self.cur_farm = Farm.objects.filter(id=self.cur_farm_id).get()
            else:
                self.cur_farm = None

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()

    def get_notification_settings(self, farm_id):
        """Get the settings defined for the different notifications
        """
        try:
            data_set = {
                'campaigns': Campaign.objects.filter(farm_id=farm_id).all(),
                'templates': MessageTemplates.objects.select_related().filter(campaign__farm_id=farm_id).order_by('template_name').all(),
                'recipients': Personnel.objects.filter(farm_id=farm_id).order_by('first_name').all(),
                'recipient_types': [k[0] for k in PERSONNEL_DESIGNATION_CHOICES],
            }

            return data_set
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def get_sent_notifications(self):
        # get the list of sent notifications
        if self.cur_user.is_superuser:
            notifications = SMSQueue.objects.select_related('recipient').select_related('template').order_by('-schedule_time').all()
        else:
            notifications = SMSQueue.objects.select_related('recipient').select_related('template').filter(recipient__farm_id=self.cur_farm_id).order_by('-schedule_time').all()

        return {'notifications': notifications}

    def save_recipient(self, request):
        try:
            # get the campaign details and add them to the database
            salutation = request.POST.get('salutation')
            first_name = request.POST.get('first-name').strip()
            other_names = request.POST.get('other-names').strip()
            designation = request.POST.get('designation')
            email = request.POST.get('email').strip()
            cell_no = request.POST.get('cell_no').strip()
            alternative_cell_no = request.POST.get('alternative_cell_no').strip() if request.POST.get('alternative_cell_no') != '' else None
            sub_county_id = request.POST.get('sub-county')
            ward_id = request.POST.get('ward').strip()
            village_id = request.POST.get('village').strip()

            # get the campaign names for this template
            if sub_county_id == '-1' or sub_county_id == '':
                sub_county = None
                ward = None
                village = None
            else:
                sub_county = SubCounty.objects.filter(id=sub_county_id).get()
                if ward_id != '' and ward_id.isnumeric() is False:
                    # we have a new ward...
                    odk_choices_parser = ImportODKChoices()
                    new_ward = {
                        'label': ward_id,
                        'name': ward_id.replace("'.- ", '').lower(),
                        'ward_subcounty': sub_county.nick_name
                    }
                    ward = odk_choices_parser.process_ward(new_ward)
                else:
                    ward = Ward.objects.filter(id=ward_id).get() if ward_id != '' else None
                
                if village_id != '' and village_id.isnumeric() is False:
                    # we have a new village...
                    odk_choices_parser = ImportODKChoices()
                    new_village = {
                        'label': village_id,
                        'name': village_id.replace("'.- ", '').lower(),
                        'village_ward': ward.nick_name
                    }
                    village = odk_choices_parser.process_village(new_village)
                else:
                    village = Village.objects.filter(id=village_id).get() if village_id != '' else None

            transaction.set_autocommit(False)
            if request.POST.get('object_id'):
                recipient = Recipients.objects.filter(id=request.POST.get('object_id')).get()
                recipient.salutation = salutation
                recipient.first_name = first_name
                recipient.other_names = other_names
                recipient.designation = designation
                recipient.cell_no = cell_no
                recipient.alternative_cell_no = alternative_cell_no
                recipient.recipient_email = email
                recipient.village = village
                recipient.ward = ward
                recipient.sub_county = sub_county
            else:
                # fabricate a nick_name for the recipient
                nick_name = '%s_%s' % (first_name.replace("'.- ", '').lower(), other_names.replace("'.- ", '').lower())
                recipient = Recipients(
                    salutation=salutation,
                    first_name=first_name,
                    other_names=other_names,
                    designation=designation,
                    cell_no=cell_no,
                    alternative_cell_no=alternative_cell_no,
                    recipient_email=email,
                    nick_name=nick_name,
                    village=village,
                    ward=ward,
                    sub_county=sub_county
                )
            recipient.full_clean()
            recipient.save()
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def save_campaign(self, request, farm_id):
        try:
            # get the campaign details and add them to the database
            campaign_name = request.POST.get('campaign-name')
            recipients = request.POST.getlist('recipients[]')
            scheduled = request.POST.get('schedule-day')
            farm = Farm.objects.filter(id=farm_id).get()

            transaction.set_autocommit(False)
            if request.POST.get('object_id'):
                # we are editing a campaign
                campaign = Campaign.objects.filter(id=request.POST.get('object_id')).get()
                campaign.campaign_name=campaign_name
                campaign.recipients=','.join(recipients)
                campaign.schedule_time=scheduled
                campaign.farm=farm
            else:
                campaign = Campaign(
                    campaign_name=campaign_name,
                    recipients=','.join(recipients),
                    schedule_time=scheduled,
                    farm=farm
                )
            campaign.full_clean()
            campaign.save()
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def send_message_immediately(self):
        # Schedule the message to be sent
        print('')

    def schedule_notification(self, template, recipient, message):
        # This function should be in the notifications module, but due to cyclic dependancies, we include it here
        try:
            cur_time = timezone.localtime(timezone.now())
            # print(message)
            # print('+254726567797' if settings.DEBUG else recipient.cell_no if recipient.cell_no else recipient.alternative_cell_no)
            queue_item = SMSQueue(
                template=template,
                message=message,
                recipient=recipient,
                recipient_no=recipient.tel if recipient.tel else recipient.alternative_tel,
                # recipient_no='+254726567797' if settings.DEBUG else recipient.tel if recipient.tel else recipient.alternative_tel,
                msg_status='SCHEDULED',
                schedule_time=cur_time.strftime('%Y-%m-%d %H:%M:%S')
            )
            queue_item.full_clean()
            queue_item.save()

            return queue_item
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception(str(e))

    def send_message_on_money_receipt(self, trans_code, amount, msisdn, first_name, middle_name, last_name):
        # for every amount received, acknowledge with a message
        try:
            transaction.set_autocommit(False)
            subscr = SubscriptionPayment.objects.select_related('tier').filter(activation_code=trans_code).first()
            if subscr is not None:
                recipient = Personnel.objects.filter(tel=subscr.subscr_init_number).first()
                # we were paying for a subscription
                if amount == subscr.subscr_amount:
                    template = MessageTemplates.objects.filter(template_name='Subscription Successful Payment').first()
                    message = template.template % (recipient.first_name, subscr.tier.tier_name)
                elif amount < subscr.subscr_amount:
                    template = MessageTemplates.objects.filter(template_name='Subscription Successful Part Payment').first()
                    # once the verification step is enabled, we shall be able to correctly determine the part payments, for now, just assume its a 2 part payment
                    balance = 'Your remaining balance is %s' % (subscr.subscr_amount - (amount + 0) )
                    message = template.template % (recipient.first_name, amount, subscr.tier.tier_name, balance)
                elif amount > subscr.subscr_amount:
                    # this is not meant to happen when the verification step is enabled, for now just accept the money
                    template = MessageTemplates.objects.filter(template_name='Subscription Successful Payment').first()
                    message = template.template % (recipient.first_name, subscr.tier.tier_name)
                    message = message + ' You have an overpayment of %s' % (subscr.subscr_amount - amount)
            else:
                # we have a general payment, check if the payer is known in the system, else create a temp account
                recipient = Personnel.objects.filter(tel__contains=msisdn).first()
                if recipient is None:
                    username = '%s_%s_%s' % (first_name, middle_name, last_name)
                    username = username.replace("'", '').replace("-", '').replace(".", '').replace(" ", '')
                    recipient = Personnel(
                        first_name=first_name,
                        last_name=last_name,
                        username=username,
                        nickname=username,
                        password=make_password(username),
                        tel='+%s' % msisdn
                    )
                    recipient.full_clean()
                    recipient.save()
                template = MessageTemplates.objects.filter(template_name='General Successful Payment').first()
                message = template.template % (recipient.first_name, amount, trans_code)
                
            notify = Notification()
            queued_item = self.schedule_notification(template, recipient, message)
            notify.configure_at()
            notify.send_via_at(queued_item)

            transaction.commit()
        except Exception as e:
            transaction.rollback()
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception('There was an error while confirming the registration.')

    def periodic_processing(self, provider):
        queue = Notification()

        if provider == 'at':
            queue.configure_at()
        elif provider == 'nexmo':
            queue.configure_nexmo()
        else:
            # configure all the providers so that they can be selected randomly
            queue.configure_at()
            queue.configure_nexmo()

        # if need be, crunch the data for feedback to the users
        # Notifications are scheduled on the specified day at settings.SENDING_SPLIT

        cur_time = timezone.localtime(timezone.now())
        i = 0
        farms = Farm.objects.all()
        kesho = date.today() + datetime.timedelta(days=1)
        for farm in farms:
            # loop through the campaigns and determine the one that needs processing
            campaigns = Campaign.objects.filter(farm_id=farm.id).all()
            daily_records = None
            admin_reports = None
            for campaign in campaigns:
                if cur_time.strftime('%A') == campaign.schedule_time or campaign.schedule_time == 'Daily':
                    templates = MessageTemplates.objects.filter(campaign_id=campaign.id).all()
                    # get the users in this campaign
                    user_groups = campaign.recipients.split(',')
                    recipients = Personnel.objects.filter(designation__in=user_groups, is_active=1, farm_id=farm.id)
                    farm_batches = Batch.objects.filter(farm_id=farm.id, exit_date=None).exclude(batch_id__icontains='general').all()

                    for template in templates:
                        # now the message
                        main_message = ''
                        
                        # check if this template should be send now...
                        parts = template.sending_time.strftime('%H:%M:%S').split(':')
                        sending_time = cur_time.replace(hour=int(parts[0]), minute=int(parts[1]), second=int(parts[2]))
                        split_seconds = (cur_time - sending_time).total_seconds()
                        # print('\nCurrent Time == %s :: Sending Time == %s\nSplits\n````````````\n%.1f -- %.1f\n--\n' % (cur_time.strftime('%A %d-%m %H:%M:%S'), template.sending_time.strftime('%A %d-%m %H:%M:%S'), split_seconds, settings.SENDING_SPLIT))

                        # if 1:
                        if split_seconds > -1 and split_seconds < settings.SENDING_SPLIT:
                            # get the templates assigned to this campaign that needs to be processed
                            if template.template_name == 'Daily Records Reminder':
                                if daily_records is None:
                                    daily_records = self.check_submitted_daily_records(farm.id)
                                # format the data for daily records reminder. loop through all records and check the ones with None
                                missing_records = {}
                                for batch_name, record in daily_records.items():
                                    # cur_record = list(record.keys())[0]
                                    for cur_record in list(record.keys()):
                                        if cur_record in ('Feed records', 'Egg production') and record[cur_record] is None:
                                            if cur_record not in missing_records:
                                                missing_records[cur_record] = []

                                            missing_records[cur_record].append(batch_name)
                                
                                for record_name, batches in missing_records.items():
                                    if len(farm_batches) == len(batches):
                                        # we are missing data for all the necessary batches
                                        main_message = '%s- %s for all batches' % ('' if main_message == '' else main_message + "\n", record_name)
                                    else:
                                        main_message = '%s- %s for %s' % ('' if main_message == '' else main_message + "\n", record_name, ', '.join(batches))

                                if main_message != '': main_message = main_message + "\n- Any other record"

                                # check if there is a scheduled event for tomorrow
                                kesho_events_narrative = self.scheduled_events(farm.id, kesho)
                                if kesho_events_narrative != '':
                                    main_message = kesho_events_narrative if main_message == '' else main_message + "\n\n" + kesho_events_narrative
                                
                                # if we have nothing to say, just keep quiet
                                if main_message == '': continue

                                # ensure that we have someone to send this message to
                                if len(recipients) == 0:
                                    # send the message to the supervisor, else to the owner
                                    recipients = Personnel.objects.filter(designation='supervisor', is_active=1, farm_id=farm.id).all()
                                    if len(recipients) == 0:
                                        recipients = Personnel.objects.filter(designation='manager', is_active=1, farm_id=farm.id).all()
                            elif template.template_name == 'Owner Daily Report':
                                if admin_reports is None: admin_reports = self.admin_daily_morning_report(farm.id)

                                main_message = ''
                                # now string all the reports together
                                if admin_reports['income_narrative'] != '':
                                    main_message = admin_reports['income_narrative']
                                if admin_reports['expense_narrative'] != '':
                                    main_message = admin_reports['expense_narrative'] if main_message == '' else "%s\n%s" % (main_message, admin_reports['expense_narrative'])
                                
                                if admin_reports['egg_narrative'] != '':
                                    main_message = admin_reports['egg_narrative'] if main_message == '' else "%s\n%s" % (main_message, admin_reports['egg_narrative'])

                                if admin_reports['deaths_narrative'] != '':
                                    main_message = admin_reports['deaths_narrative'] if main_message == '' else "%s\n%s" % (main_message, admin_reports['deaths_narrative'])

                                if main_message == '': main_message = ' No data recorded'

                                # this should be the last part of the message
                                if admin_reports['kesho_events_narrative'] != '':
                                    main_message = admin_reports['kesho_events_narrative'] if main_message == '' else "%s\n\n%s" % (main_message, admin_reports['kesho_events_narrative'])
                            elif template.template_name == 'Supervisor Daily Report':
                                if admin_reports is None: admin_reports = self.admin_daily_morning_report(farm.id)
    
                                # now string all the reports together
                                if admin_reports['egg_narrative'] != '':
                                    main_message = admin_reports['egg_narrative']

                                if admin_reports['deaths_narrative'] != '':
                                    main_message = admin_reports['deaths_narrative'] if main_message == '' else "%s\n%s" % (main_message, admin_reports['deaths_narrative'])

                                # this should be the last part of the message
                                if admin_reports['kesho_events_narrative'] != '':
                                    main_message = admin_reports['kesho_events_narrative'] if main_message == '' else "%s\n\n%s" % (main_message, admin_reports['kesho_events_narrative'])

                                if main_message == '': main_message = ' No data recorded'
                            
                            for recipient in recipients:
                                message = template.template % (recipient.first_name, main_message)

                                if recipient.tel:
                                    print('\nSending %s to %s \n%s' % (template.template_name, recipient.first_name, message))
                                    self.schedule_notification(template, recipient, message)
                                    i = i + 1

        queue.process_scheduled_sms(provider)
        print('\nSent %d messages\n' % i)

    def check_submitted_daily_records(self, farm_id):
        # check if the expected daily records are submitted
        # 1. Feed records
        # 2. Egg production

        records = {}
        today = datetime.datetime.now()
        kesho = date.today() + datetime.timedelta(days=1)
        batches = Batch.objects.filter(farm_id=farm_id, exit_date=None).exclude(batch_id__icontains='general').all()
        for batch in batches:
            cur_batch_name = self.format_batch_name(batch.batch_id, batch.batch_name)
            # cur_batch_name = ' '.join(batch.batch_name.split(' - ')[:2])
            records[cur_batch_name] = {}
            feed_record = OtherEvents.objects.filter(event_type='Feed Increment', batch_id=batch.id, event_date=today.strftime("%Y-%m-%d")).first()
            records[cur_batch_name]['Feed records'] = None if feed_record is None else feed_record.event_val
            
            # lets look for a egg production record if need be
            egg_prod = Production.objects.filter(product='Egg Production', batch_id=batch.id).count()
            if egg_prod != 0:
                today_egg_prod = Production.objects.filter(product='Egg Production', batch_id=batch.id, date_produced=today.strftime("%Y-%m-%d")).first()
                records[cur_batch_name]['Egg production'] = None if today_egg_prod is None else today_egg_prod.no_units

            # check for deaths
            deaths = OtherEvents.objects.filter(event_type='Deaths', batch_id=batch.id, event_date=today.strftime("%Y-%m-%d")).first()
            records[cur_batch_name]['Deaths'] = None if deaths is None else deaths.event_val

        return records

    def admin_daily_morning_report(self, farm_id):
        # process the manager's daily morning report of the previous day's activities
        # 1. Income: KShs. %f,
        # 2. Expenses: KShs. %f,
        # 3. Eggs laid: %d (Batch01: %d, Batch02 %d),
        # 4. Broilers sold: %d @ KShs %s, total %f,
        # 5. Kienyeji sold: %d @ KShs %s, total %f,
        # 6. Deaths: %d (Batch01: %d, Batch02 %d)
        
        try:
            records = {}
            yday = date.today() + datetime.timedelta(days=-1)
            kesho = date.today() + datetime.timedelta(days=1)
            ie_dates = [yday.strftime("%Y-%m-%d")]
            poultry_batches = Batch.objects.filter(farm_id=farm_id).exclude(batch_id__icontains='general').values('id').all()
            batches_ids = [f['id'] for f in poultry_batches]

            all_batches = Batch.objects.filter(farm_id=farm_id).values('id').all()
            batches_incl_gen = [f['id'] for f in all_batches]

            # income .. lets use 2019-05-24 which has a lot of entries
            # Sales dates
            # ie_date       count(*)
            # 2019-08-07    5
            # 2019-12-18    4
            # 2019-12-21    4
            # 2019-12-25    4
            # 2020-01-04    4
            # 2020-02-05    4
            # 
            # Expense dates
            # ie_date       count(*)
            # 2019-02-01    7
            # 2019-05-24    10
            # 2019-08-05    8
            # 2019-09-18    6
            # ie_dates = ['2019-08-07', '2019-12-18', '2019-12-21', '2019-12-25', '2020-01-04', '2020-02-05', '2019-02-01', '2019-05-24', '2019-08-05', '2019-09-18']

            # process the incomes and expenses
            # yday_incomes = IncomeExpense.objects.filter(ie_date=yday.strftime("%Y-%m-%d"), ie_type='Sale', batch_id__in=batches_incl_gen).all()
            yday_incomes = IncomeExpense.objects.filter(ie_date__in=ie_dates, batch_id__in=batches_incl_gen).all()
            total_income = 0
            total_expense = 0
            for inc in yday_incomes:
                # print("%s - %s - %d - %f" % (inc.ie_name, inc.ie_type, inc.no_units, inc.unit_cost))
                this_total = inc.no_units * inc.unit_cost
                
                if inc.ie_type == 'Sale':
                    total_income = total_income + (this_total / 2) if re.match('^Half', inc.ie_name) else total_income + this_total
                elif inc.ie_type == 'Expense':
                    total_expense = total_expense + this_total

            income_narrative = "Income: KShs. %s" % '{:,.1f}'.format(total_income) if total_income > 0 else ''
            expense_narrative = "Expense: KShs. %s" % '{:,.1f}'.format(total_expense) if total_expense > 0 else ''

            # get the eggs laid
            egg_prod = Production.objects.select_related('batch').filter(date_produced__in=ie_dates, batch_id__in=batches_ids).values('batch__batch_id').annotate(total_eggs=Sum('no_units'))
            if len(egg_prod) == 0:
                egg_narrative = ''
            elif len(egg_prod) == 1:
                egg_narrative = "%d Eggs laid (%s)" % (egg_prod[0]['total_eggs'], egg_prod[0]['batch__batch_id'].capitalize())
            else:
                narrative = ', '.join(["%s: %d" % (ep['batch__batch_id'].capitalize(), ep['total_eggs']) for ep in egg_prod])
                a = [ep['total_eggs'] for ep in egg_prod]
                total_eggs = sum(map(lambda x:x,a))
                egg_narrative = "%d Eggs laid (%s)" % (total_eggs, narrative)

            # get the deaths
            all_deaths = OtherEvents.objects.select_related('batch').filter(event_date__in=ie_dates, event_type='Deaths', batch_id__in=batches_ids).values('batch__batch_id').annotate(total_deaths=Sum('event_val'))
            if len(all_deaths) == 0:
                deaths_narrative = ''
            elif len(all_deaths) == 1:
                deaths_narrative = "%d Deaths (%s)" % (all_deaths[0]['total_deaths'], all_deaths[0]['batch__batch_id'].capitalize())
            else:
                narrative = ', '.join(["%d Deaths (%s)" % (ep['total_deaths'], ep['batch__batch_id'].capitalize()) for ep in all_deaths])
                a = [ep['total_deaths'] for ep in all_deaths]
                total_deaths = sum(map(lambda x:x,a))
                deaths_narrative = "%d Deaths (%s)" % (total_deaths, narrative)

            # check if there is a scheduled event for tomorrow
            kesho_events_narrative = self.scheduled_events(farm_id, kesho)

            return {'income_narrative':income_narrative, 'expense_narrative':expense_narrative, 'egg_narrative':egg_narrative, 'deaths_narrative':deaths_narrative, 'kesho_events_narrative':kesho_events_narrative}
        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            raise Exception("There was an error while processing the manager's daily report")

    def scheduled_events(self, farm_id, event_date):
        # check if we have any events scheduled for tomorrow,
        # if there is, format them as notifications
        #print(event_date.strftime("%Y-%m-%d"))
        farm_batches = Batch.objects.filter(farm_id=farm_id).exclude(batch_id__icontains='general').values('id').all()
        batches_ids = [f['id'] for f in farm_batches]
        events = EventsSchedule.objects.select_related('event', 'batch').filter(batch_id__in=batches_ids, schedule_date=event_date.strftime("%Y-%m-%d")).all()

        kesho_events_narrative = ''
        for ev in events:
            cur_batch_name = self.format_batch_name(ev.batch.batch_id, ev.batch.batch_name)
            if ev.event.event_type == 'Vaccination': cur_narrative = "vaccinate %s against %s" % (cur_batch_name, ev.event.event_name)
            elif ev.event.event_type == 'Deworming': cur_narrative = "deworm %s" % cur_batch_name
            elif ev.event.event_name == 'Weighing': cur_narrative = "weigh %s" % cur_batch_name
            else: cur_narrative = "%s %s" % (ev.event.event_name, cur_batch_name)

            kesho_events_narrative = cur_narrative if kesho_events_narrative == '' else kesho_events_narrative + ', ' + cur_narrative
        
        if kesho_events_narrative != '': kesho_events_narrative = "Remember tomorrow to %s" % kesho_events_narrative

        return kesho_events_narrative

    def format_batch_name(self, batch_id, batch_name):
        # given a batch name, try and compress it to save characters in an SMS
        a = re.match('^(Batch\s\d+).+', batch_name)
        if a:
            return a[1]
        else:
            batch_id.capitalize()


class BoxGirlsNotification():
    def __init__(self, cur_user_email=None):
        self.time_formats = ['now', 'today', 'tomorrow', 'yesterday']
        self.at_ok_sending_status_codes = [100, 101, 102]

    def periodic_processing(self, provider):
        queue = Notification()

        if provider == 'at':
            queue.configure_at()
        elif provider == 'nexmo':
            queue.configure_nexmo()
        else:
            # configure all the providers so that they can be selected randomly
            queue.configure_at()
            queue.configure_nexmo()


class ChurchRegisterNotification():
    def __init__(self, cur_user_email=None):
        self.time_formats = ['now', 'today', 'tomorrow', 'yesterday']
        self.at_ok_sending_status_codes = [100, 101, 102]


