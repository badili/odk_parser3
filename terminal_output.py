

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class Terminal():
    def __init__(self):
        self.HEADER = '\033[95m'
        self.OKBLUE = '\033[94m'
        self.OKGREEN = '\033[92m'
        self.WARNING = '\033[93m'
        self.FAIL = '\033[91m'
        self.ENDC = '\033[0m'
        self.BOLD = '\033[1m'
        self.UNDERLINE = '\033[4m'

    def tprint(self, message, msg_type):
        if msg_type == 'warn':
            tcolor = bcolors.WARNING
        elif msg_type == 'ok':
            tcolor = bcolors.OKGREEN
        elif msg_type == 'okblue':
            tcolor = bcolors.OKBLUE
        elif msg_type == 'underline':
            tcolor = bcolors.UNDERLINE
        elif msg_type == 'fail':
            tcolor = bcolors.FAIL
        elif msg_type == 'header':
            tcolor = bcolors.HEADER
        elif msg_type == 'info':
            tcolor = bcolors.OKBLUE
        elif msg_type == 'debug':
            tcolor = bcolors.WARNING
        elif msg_type == 'error':
            tcolor = bcolors.FAIL
        else:
            tcolor = bcolors.OKGREEN

        print((tcolor + message + bcolors.ENDC))
