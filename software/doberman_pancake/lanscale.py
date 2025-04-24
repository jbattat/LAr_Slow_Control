from Doberman import LANDevice, utils
import re
import socket
import time

class lanscale(LANDevice):
    """
    Scale for nitrogen dewar in pancake.
    """
    eol = b'\n'
    xtrem_remotep = 5555
    msg_wait = 1
    recv_interval = 0.01
    max_msgs_wrong_ip = 20
    commands = {
        'tare': '0001E01020000',
        'untare': '0001E11030000',
        'zero': '0001E10300000'
    }

    def process_one_value(self, name=None, data=None):
        """
        Takes the raw data as returned by send_recv and parses
        it for the float.
        """
        return float(re.search('(?P<value>\-?[0-9]+(?:\.[0-9]+)?)kg', data).group('value'))


    def setup(self):
        self.packet_bytes = 1024
        self._device = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._device.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._device.settimeout(0) # We will manually sleep to avoid blocking other threads
        self._device.bind(('0.0.0.0', self.xtrem_remotep))
        self._msg_start = '\u0002'
        self._msg_end = '\u0003\r\n'
        return True

    def shutdown(self):
        pass

    def execute_command(self, quantity, value):
        if quantity.startswith('0001R'):
            # Allow direct command input only for read commands
            return quantity
        else:
            # Otherwise look up from the command dictionary
            return self.commands[quantity]

    def recv_or_none(self):
        try:
            data, fromaddress = self._device.recvfrom(self.packet_bytes)
            return data.decode(), fromaddress
        except BlockingIOError:
            return None, None

    def send_recv(self, message):
        sendrecvstart = time.time()
        # UDP not TCP so need a special method
        ret = {'retcode': 0, 'data': None}

        message = str(message).rstrip()
        wrappedmessage = self._msg_start + message + self._msg_end

        # First empty the buffer so we don't get old readings
        data = 0
        while data is not None:
            data, fromaddress = self.recv_or_none()
        
        # Now send our request for a reading
        try:
            self._device.sendto(wrappedmessage.encode(), (self.ip, self.port))
        except socket.error as e:
            self.logger.fatal("Could not send message %s. Error: %s" % (message.strip(), e))
            ret['retcode'] = -2
            return ret

        # Determine string we need to search for in return
        responsesearchpattern = message[2:4] + message[0:2] + message[4].lower() + message[5:9]
        for i in range(int(self.msg_wait / self.recv_interval) + 1):
           data, fromaddress = self.recv_or_none()
           if fromaddress == (self.ip, self.port) and responsesearchpattern in data:
               ret['data'] = data
               return ret
           time.sleep(self.recv_interval)
        raise IOError("Never got the expected response from the scale.")
