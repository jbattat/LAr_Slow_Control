from Doberman import CheapSocketDevice, utils
import re

class Waveshare4(CheapSocketDevice):
    """
    Waveshare RS-232/485 to POE ethernet converter (1 channel) used to readout HUM, TMP, DEW of the purifier
    """

    def set_parameters(self):
        #R\r\n
        self._msg_end = '\r'
        self._msg_start = ''
       
        #self.value_pattern = re.compile(f'(?P<value>{utils.number_regex})'.encode())
        

    def process_one_value(self, name=None, data=None):
        """
        Takes the raw data as returned by send_recv and parses
        it for the float. Only for the scales.
        """
        print(f"Data: {data}")

        #val =  data.decode('UTF-8').strip()
        
        vals = data.decode('utf-8').strip('\r').split(',')
        hum = float(vals[0])
        tmp = float(vals[1])
        dew = float(vals[3])
        return hum,tmp,dew
