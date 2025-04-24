from Doberman import LANDevice, CheapSocketDevice, utils
import re

#class OmegaDPI8(LANDevice):
class OmegaDPI8(CheapSocketDevice):
    """
    Omega DPI8 Process meter to read out the Pressure transducer
    """
    #eol = b'\r\x03'  # default is \r for LANDevice

    def set_parameters(self):
        self._msg_end = '\r'
        self._msg_start = '*'
        #self.value_pattern = re.compile(f'(?P<value>{utils.number_regex})'.encode())
    #    pass
    
    #def setup(self):
    #    """ Called right after establishing a connection to the device. 
    #    Useful if, for instance, the device automatically sends data continuously, and you want it to stop. 
    #    The full message sent is self._msg_start + 'stop' + self._msg_end"""
    #    #pass
    #    print("\n\n\ni'm an idiot\n\n")
    #    #self.send_recv('stop')

    def process_one_value(self, name=None, data=None):
        """
        Takes the raw data as returned by send_recv and parses
        it for the float. Only for the scales.
        """
        print(data)
        val = data.decode('UTF-8').removeprefix('01X01')
        print(f'\n\n{val}\n\n')
        return float(val)


    #def execute_command(self, quantity, value):
    #    """ useful to interact with the device during a run -- e.g. change setpoint for an alarm """
    #    pass
    #    #if quantity == 'setpoint':
    #    #    return f'setpt:{value}'
