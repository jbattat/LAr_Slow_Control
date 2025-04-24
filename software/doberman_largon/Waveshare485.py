from Doberman import CheapSocketDevice, utils
import re

class Waveshare485(CheapSocketDevice):
    """
    Waveshare RS-485-to-ethernet converter (4 channel) used to readout the Hornet pressure sensors on the cryostat
    """

    def set_parameters(self):
        self._msg_end = '\r'
        self._msg_start = '#'
        #self.value_pattern = re.compile(f'(?P<value>{utils.number_regex})'.encode())

    def process_one_value(self, name=None, data=None):
        """
        Takes the raw data as returned by send_recv and parses
        it for the float. Only for the scales.
        """
        print(data)
        val = data.decode('UTF-8').removeprefix('*01')
        #val = data.decode('UTF-8').strip()[4:]
        print(f'\n\nWaveshare485 value: {val}\n\n')
        return float(val)

