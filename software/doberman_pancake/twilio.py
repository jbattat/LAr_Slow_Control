from Doberman import Device, AlarmMonitor, utils
import requests
import re

class HTTPDevice(Device):
    """
    Accesses information available over an HTTP request.

    The possible commands should be set by the
    set_parameters function of the child class.
    This should be a dictionary, each key is a command.
    The entry is a dictionary containing the keys:
      - type: the type of HTTP request ('get' or 'post')
      - url: the url of the request
      - data (optional): passed as the data to the requests call
      - auth (optional) HTTP basic authentication info
    """

    def send_recv(self, message):
        ret = {'retcode': 0, 'data': []}
        command = self.commands[message]
        url = command['url']
        data = command.get('data')
        auth = command.get('auth')
        if isinstance(auth, list):
            auth = tuple(auth)
        if command['type'] == 'post':
            requests_function = requests.post
        elif command['type'] == 'get':
            requests_function = requests.get
        else:
             raise ValueError(f"Unsupported request type {command['type']}")
        self.logger.debug(f"Url: {url}")
        self.logger.debug(f"Data: {data}")
        response = requests_function(url=url, data=data, auth=auth)
        self.logger.debug(f"Received {response.content.decode()} from request")
        ret['data'] = response.content.decode()
        return ret

class twilio(HTTPDevice):
    """
    Checks for balance from twilio
    """

    def set_parameters(self):
        self.logger.debug("Setting parameters")
        self.commands = {
            'credits': {
                'type': 'get',
                'url': f'https://api.twilio.com/2010-04-01/Accounts/{self.params["account"]}/Balance.json',
                'auth': self.params['auth']
            }
        }
        self.value_pattern = re.compile(f'"balance": "(?P<value>{utils.number_regex})"')
