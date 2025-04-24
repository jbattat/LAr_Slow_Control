from Doberman import LANDevice, utils
import re

class scale(LANDevice):
    """
    Scale for nitrogen dewar in pancake.
    """
    eol = b'\r\x03'

    def process_one_value(self, name=None, data=None):
        """
        Takes the raw data as returned by send_recv and parses
        it for the float. Only for the scales.
        """
        data = re.sub('\s','',data.decode())
        return float(re.search('(?P<value>\-?[0-9]+(?:\.[0-9]+)?)kg', data).group('value'))
