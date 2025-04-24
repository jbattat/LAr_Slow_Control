from Doberman import LANDevice, utils
import re

class wut(LANDevice):
    """
    W&T Web-Thermometer (57708)
    """
    _msg_start = 'GET /Single'
    eol = b'\x00'

    def process_one_value(self, name=None, data=None):
        assert data[-1] == 0
        datasplit = data[:-1].decode('unicode_escape').split(';')
        return [float(x[:-2].replace(',','.')) if x[:-2] != '----' else None for x in datasplit]
