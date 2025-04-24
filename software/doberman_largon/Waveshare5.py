from Doberman import CheapSocketDevice, utils
import re

class Waveshare5(CheapSocketDevice):
    """
    Waveshare RS-232/485 to POE ethernet converter (1 channel) used to readout The Stanford Research Systems SR630 Thermocouple Readout
    """

    def set_parameters(self):
        #R\r\n
        self._msg_end = '\r'
        self._msg_start = ''
        self.value_pattern = re.compile(f'(?P<value>{utils.number_regex})'.encode())


    #debug? by emptying buffer at the beginning
    def setup(self):
        self.send_recv('1024')

      
    def process_one_value(self, name=None, data=None):
        """
        Takes the raw data as returned by send_recv and parses
        it for the float. Only for the scales.
        """
        print(f"Data: {data}")

        #val =  data.decode('UTF-8').strip()
        
        vals = data.decode('utf-8').strip('\r').split(';')
        print(vals)
        

        #to do: check if data is in the correct format "T7;T8\r\n'

        
        if len(vals)<2:
            t1, t2 = 0, 0
            print('missing values')
        else:
            t1 = float(vals[0])
            t2 = float(vals[1])
        
        
        return t1, t2
