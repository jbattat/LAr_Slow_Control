from Doberman import LANDevice, utils
import re

class teledyne_hfc(LANDevice):
    """
    Teledyne mass flow controller.
    For digital 300B series flow controllers.
    """
    eol = b'\r>'
    _msg_end = '\r'
    commands = {  # these are not case sensitive
        'set_setpoint': 'V4={value}',
        'set_valvemode': 'V1={value}'
    }
    value_pattern = re.compile(f'(?P<value>{utils.number_regex})'.encode())

    def execute_command(self, quantity, value):
        if quantity == 'setpoint':
            return self.commands['set_setpoint'].format(value=value)
        elif quantity == 'valvemode':
            value = int(value)
            if value not in range(6):
                raise ValueError('Only allowable valve modes are 0 to 5')
            return self.commands['set_valvemode'].format(value=value)
