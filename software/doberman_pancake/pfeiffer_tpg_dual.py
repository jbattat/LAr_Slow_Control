from Doberman import LANDevice, utils
import re  # EVERYBODY STAND BACK xkcd.com/208


class pfeiffer_tpg_dual(LANDevice):
    eol = b'\n'
    _msg_end = '\r\n\x05'
    commands = {
        'identify' : 'AYT',
        }
    value_pattern = re.compile(('(?P<status>[0-9]),(?P<value>%s)' %
                                              utils.number_regex).encode())
