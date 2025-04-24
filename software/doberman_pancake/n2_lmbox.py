from Doberman import SerialDevice
import struct
import time
import socket


class n2_lmbox(SerialDevice):
    """
    Custom level meter box for pancake. Read via Serial connection at the moment
    """

    def set_parameters(self):
        self.eol = b'\r'
        self.split = b'\x06'

    def setup(self):

        self.send_recv('0')
        time.sleep(1)
        self.send_recv('1')
        time.sleep(1)

    def process_one_value(self, name, data):
        """
        Data structure: 6 times 4 integers divided by the split character plus the EOL-character.
        """
        self.logger.debug(f'{data}')
        if self.eol in data:
            data = data.split(self.eol)[0]
        else:
            self.logger.debug('EOL not found')
            return
        if len(data) != 54:
            self.logger.debug(f'data length is {len(data)} not 54')
            data = self.salvage_input(data)
        if len(data) != 54:
            self.logger.debug(f'salvaging unsuccessful, length is {len(data)} not 54')
            return
        decoded = struct.unpack('<' + 6 * (4 * 'h' + 'c'), data)
        c_meas = []
        for i in range(6):
            lm_values = decoded[5 * i:5 * i + 4]
            offset_values = sorted(lm_values)[:2]
            n_off = sum(offset_values)
            index_min = lm_values.index(min(offset_values))
            index = (index_min + 2) % 4 if lm_values[(index_min + 1) % 4] in offset_values else (index_min + 1) % 4
            n_ref = lm_values[index]
            n_x = lm_values[(index + 1) % 4]
            c_meas.append(self.params['c_ref'][i] * (n_x - n_off) / (n_ref - n_off))
        return c_meas

    def salvage_input(self, data):
        """
        This may or may not salvage some reading if the length of the data doesn't fit the normal
        structure.
        """
        place_holder = b'\x02\x00\x02\x00\x05\x00\x03\x00'
        data_temp = data.split(self.split)[:-1]
        data_list = [p if len(p) == 8 else place_holder for p in data_temp]
        data = self.split.join(data_list) + self.split
        return data
