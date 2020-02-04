# -*- coding: utf-8 -*-


import six
# import sys
import os
import csv
import re

from vendor.terminal_output import Terminal
from pyexcelerate import Workbook

terminal = Terminal()
# reload(sys)
# sys.setdefaultencoding('utf8')


class ExcelWriter():
    def __init__(self, workbook_name, this_format='xlsx', directory='./'):
        self.wb_name = workbook_name
        self.format = this_format
        self.save_dir = directory
        self.pending_processing = {}
        self.pending_processing_tmp = {}
        self.sorted_sheet_fields = {}

    def create_workbook(self, data, structure):
        # given the data as json and the structure as json too, create a workbook with this data

        self.wb = Workbook()
        # order the fields for proper display
        for sheet_name, sheet_fields in six.iteritems(structure):
            self.sorted_sheet_fields[sheet_name] = self.order_fields(sheet_fields)

        self.pending_processing['main'] = {'data': data, 'is_processed': False}

        # from the docs: https://docs.python.org/2.7/tutorial/datastructures.html#dictionaries
        # It is sometimes tempting to change a list while you are looping over it;
        # however, it is often simpler and safer to create a new list instead
        # so lets not change the list while iterating
        while len(list(self.pending_processing.keys())) != 0:
            all_processed = True
            for sheet_name, data in six.iteritems(self.pending_processing):
                if data['is_processed'] is False:
                    terminal.tprint('Processing ' + sheet_name, 'okblue')
                    all_processed = False
                    self.process_and_write(data['data'], sheet_name)
                    # mark it as processed
                    self.pending_processing[sheet_name]['is_processed'] = True
                    break

            if all_processed is True:
                break

        if self.format == 'xlsx':
            self.wb.save(self.wb_name)
        return False

    def process_and_write(self, data, sheet_name):
        # contains 2D array of all the data for the current sheet
        cur_records = []
        cur_records.append(self.sorted_sheet_fields[sheet_name])

        for record in data:
            # contains 1D array of data for the current record data
            this_record = []
            for field in self.sorted_sheet_fields[sheet_name]:
                try:
                    cur_value = record[field]
                except KeyError:
                    cur_value = '-'

                if isinstance(cur_value, list) is True:
                    # defer processing
                    if field not in self.pending_processing:
                        terminal.tprint("\tFound new sheet (%s) data to save. Deferring for now." % field, 'warn')
                        self.pending_processing[field] = {'data': [], 'is_processed': False}

                    self.pending_processing[field]['data'].extend(cur_value)
                    # add a link
                    this_record.append('Check ' + field)
                else:
                    this_record.append(cur_value)

            cur_records.append(this_record)

        # now lets do a batch write of our data
        # terminal.tprint("\tBatch writing of " + sheet_name, 'ok')
        # terminal.tprint(json.dumps(cur_records), 'warn')
        if self.format == 'xlsx':
            if len(sheet_name) > 31:
                # if the worksheet name is > 31 chars rename it to a shorter name due to Excel worksheet name restrictions
                # https://stackoverflow.com/questions/3681868/is-there-a-limit-on-an-excel-worksheets-name-length
                # we rename the worksheet by retaining the first 2 parts from s7p10q1_rpt_chicken_hlth_service ==> s7p10q1_rpt
                matches = re.findall(r'^(s[\d_\.]+p[\d_\.]+q[\d_\.]+?_)(rpt)?', sheet_name)
                matches = list(matches[0])
                if len(matches) == 2:
                    final_sheet_name = str(matches[0]) + str(matches[1])
                elif len(matches) == 1:
                    final_sheet_name = str(matches[0])
            else:
                final_sheet_name = sheet_name
            self.wb.new_sheet(final_sheet_name, data=cur_records)

        if self.format == 'csv':
            filename = os.path.join(self.save_dir, '%s.%s' % (sheet_name, 'csv'))
            with open(filename, "w") as f:
                terminal.tprint("Saving the file %s" % filename, 'warn')
                writer = csv.writer(f, delimiter=',', quotechar='"')
                for row in cur_records:
                    writer.writerow([s for s in row])

    def order_fields(self, fields):
        fields.sort()

        # remove the parent_id if its there and append it at the begining
        if 'parent_id' in fields:
            fields.remove('parent_id')
            fields.insert(0, 'parent_id')

        if 'top_id' in fields:
            fields.remove('top_id')
            fields.insert(0, 'top_id')

        fields.remove('unique_id')
        fields.insert(0, 'unique_id')

        return fields
