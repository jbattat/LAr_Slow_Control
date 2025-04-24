from Doberman import Device
import time
import fcntl
import struct

class pt100mux(Device):
    """
    Plug-in for PT100MUX multiplexer. 
    This works together with a given RevPi and must be executed on this RevPi.
    The plugin sends a series of digital inputs to the Multiplexer (0-7) to change the channel.
    For each channel setting the output is read out and converted to degree Celcius.
    additional_params tp be saved in sensor document in DB:
        digital_inputs: array of the names of DO channels used.
                        e.g. ['O_1', 0_2', 'O_3'], where O_1 is connected to PT100MUX input 1 etc.
        analog_output:  name of the analog input (of the RevPi), the analog output of the multiplexer is
                        connected to.
    """
    
    def set_parameters(self):
        self.analog_pos = self.find_pos_by_name(self.params['analog_output'])
        self.analog_offset = struct.unpack_from('>H', self.analog_pos, 32)[0] >> 8
    

    def send_recv(self, message):
        ret = {'retcode': 0, 'data': []}
        for i in range(8):
            self.switch_channel(i)
            self.event.wait(1.5)  # Let analog channel adjust to new value
            with open('/dev/piControl0', 'rb+', 0) as f:
                f.seek(self.analog_offset)
                ret['data'].append(int.from_bytes(f.read(2), 'little'))
        self.logger.debug(f'currents: {ret["data"]}')
        return ret


    def switch_channel(self, i):
        """
        Switches the channel of the multiplexer between 0 and 7.
        :param i: channel number
        """
        self.logger.debug(f'switching to channel {i}')
        bin_str = format(i, '03b')[::-1]  # 0='000', 1='100', 2='010', 3='110', ..., 7='111'
        self.logger.debug(f' binary string: {bin_str}')
        with open('/dev/piControl0', 'wb+', 0) as f:
            for j in range(3):
                bit_pos = self.find_pos_by_name(self.params['digital_inputs'][j])
                self.write_bit(bit_pos, int(bin_str[j]))


    def find_pos_by_name(self, name):
        """
        Finds the position information of a parameter in the proccess image
        :param name: symbolic name of the parameter (defined in piCtory)
        :returns:   packed byte array which contains symbolic name (32 bytes), offset (2 bytes),
                    bit (1 byte), and length (2 bytes)
        """
        prm = (b'K'[0]<<8) + 17  # function to find a variable by the name defined in piCtory  
        struct_name = struct.pack('37s', name.encode())
        with open('/dev/piControl0', 'wb+', 0) as f:
            ret = fcntl.ioctl(f, prm, struct_name)
        return ret


    def write_bit(self, pos, bitval):
        """
        Overwrites a single bit in the process images
        :param pos: packed array containing position information of the bit. 
                    Contains name (32 bytes), offset (2 bytes),bit (1 byte), and length (2 bytes)
        :param bitval: Value to write to the given position
        """
        prm = (b'K'[0]<<8) + 16  # function to change single bit
        value = bytearray([0,0,0,0])
        offset = struct.unpack_from('>H', pos, 32)[0]
        bit = struct.unpack_from('B', pos, 34)[0]
        struct.pack_into('>H', value, 0, offset)
        struct.pack_into('B', value, 2, bit)
        struct.pack_into('B', value, 3, bitval)
        with open('/dev/piControl0', 'wb+', 0) as f:
            fcntl.ioctl(f, prm, value)


    def process_one_value(self, name, data):
        """
        Convert current measurement [4000-20000]uA to temperature [-200,300]degC.
        Values below 4000uA are set to -1000dC
        :param name: name of the reading
        :param data: array of 8 current values in uA
        :returns: array of currents converted to temperatures
        """
        temperatures = list(map(lambda x: 1/32 * (x - 10400), data))
        return temperatures
