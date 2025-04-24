from Doberman import LANDevice
import struct
import time
import socket


class n2_lmbox_lan(LANDevice):
    """
    Custom level meter box for pancake. The device is read out through an RS485 to ETH adapter, that's why
    it inherits from LANSensor. send_recv() is modified to sleep longer since the device reacts slower than
    a standard LAN sensor.
    """

    msg_wait = 2
    eol = b'\r'
    split = b'\x06'

    def process_one_value(self, name, data):
        """
        Data structure: 6 times 4 integers divided by the split character plus the EOL-character.
        """
        with open('/global/logs/pancake/special/lmtest.bin', 'ab') as f:
            f.write(data + b'\n')
        if not data.endswith(self.eol):
            self.logger.info(f'Data does not end with EOL but with {data[-1]}')
        if len(data) == 55:
            # If it is the right length, split by position since reading might contain \x06
            data = [data[i:i+8] for i in range(0,54,9)]
        else:
            # Otherwise split by splitting character
            data = data.split(self.split)[:-1] # Remove EOL
        if len(data) != 6:
            self.logger.debug(f'Data contains {len(data)} readings, not 6')
            return None

        c_meas = []
        for i, readingdata in enumerate(data):
            try:
                lm_values = struct.unpack('<hhhh', readingdata) # Each packet is four shorts

                # The two smallest values are the offsets
                # Order always the same, but starting value changes
                offset_values = sorted(lm_values)[:2]
                n_off = sum(offset_values)
                index_min = lm_values.index(min(offset_values))
                index = (index_min + 2) % 4 if lm_values[(index_min + 1) % 4] in offset_values else (index_min + 1) % 4
                n_ref = lm_values[index]
                n_x = lm_values[(index + 1) % 4]

                c_meas.append(self.params['c_ref'][i] * (n_x - n_off) / (n_ref - n_off))
            except Exception as e:
                self.logger.debug(f'Problem interpreting capacitance value {i}, {e}')
                c_meas.append(None)
        return c_meas

