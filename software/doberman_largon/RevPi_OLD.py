import fcntl
import struct
import time
from Doberman import Device


class RevPi(Device):
    """
    Class for RevolutionPi devices
    """

    def set_parameters(self):
        self.commands = {
            'write': 'w {name} {value}'
        }
        self.positions = {}
        self.targets = {'PM_ANODE_HV_Vmon': 'AnalogInput_1', }
        #self.keywords = {'open': 1, 'close': 0, }

    def shutdown(self):
        self.f.close()

    def setup(self):
        self.f = open('/dev/piControl0', 'wb+', 0)

    def get_position(self, name):
        prm = (b'K'[0] << 8) + 17
        struct_name = struct.pack('37s', name.encode())
        ret = fcntl.ioctl(self.f, prm, struct_name)
        return ret

    def write(self, name, value):
        """
        Set the value of a variable (most likely an output)
        :param name: name of the variable as defined in the Pictory
        :param value: value to be set
        """
        if name not in self.positions:
            self.positions[name] = self.get_position(name)
        offset = struct.unpack_from('<H', self.positions[name], 32)[0]
        length = struct.unpack_from('B', self.positions[name], 36)[0]
        prm = (b'K'[0] << 8) + 16
        byte_array = bytearray([0, 0, 0, 0])
        if length == 1:  # single bit
            bit = struct.unpack_from('B', self.positions[name], 34)[0]
            struct.pack_into('<H', byte_array, 0, offset)
            struct.pack_into('B', byte_array, 2, bit)
            struct.pack_into('B', byte_array, 3, int(value))
            fcntl.ioctl(self.f, prm, byte_array)
        else:  # writing 2 bytes
            self.f.seek(offset)
            self.f.write(int(value).to_bytes(2, 'little'))

    def read(self, name):
        """
        Read value of a variable
        :param name: name of the variable as defined in the Pictory
        """
        if name not in self.positions:
            self.positions[name] = self.get_position(name)
      
       
        value = bytearray([0, 0, 0, 0])
        offset = struct.unpack_from('<H', self.positions[name], 32)[0]
        length = struct.unpack_from('B', self.positions[name], 36)[0]
        prm = (b'K'[0] << 8) + 15
        if length == 1:  # single bit
            bit = struct.unpack_from('B', self.positions[name], 34)[0]
            struct.pack_into('<H', value, 0, offset)
            struct.pack_into('B', value, 2, bit)
            fcntl.ioctl(self.f, prm, value)
            ret = value[3]
        else:  # two bytes
            with open('/dev/piControl0', 'rb+') as f:
                f.seek(offset)
                ret = int.from_bytes(f.read(2), 'little', signed=True)
        self.logger.debug(f'{name}: {ret}')
        return ret

    def execute_command(self, target, value):

        target = self.targets.get(target, target)
        value = self.keywords.get(value, value)
        return self.commands['write'].format(name=target, value=value)

    def send_recv(self, message):
        ret = {'retcode': 0, 'data': None}
        msg = message.split()  # msg = <r> <name> | <w> <name> <value>
        if msg[0] == 'r':
            ret['data'] = self.read(msg[1])
        elif msg[0] == 'w':
            if int(msg[2]) > (1 << 16):
                pass
            self.write(msg[1], msg[2])
        else:
            self.logger.error(f"Message starts with invalid character {msg[0]}. Allowed characters: 'r' or 'w'")
            ret['retcode'] = -1
        return ret

    def process_one_value(self, name, data):
        """
        Do nothing. Leaves conversion to sensible units to later.
        """
        return data
