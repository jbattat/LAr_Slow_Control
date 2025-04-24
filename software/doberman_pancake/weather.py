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
        response = requests_function(url=url, data=data, auth=auth)
        ret['data'] = response.content.decode()
        return ret

class weather(HTTPDevice):
    def set_parameters(self):
        self.commands = {
            'weather': {
                'type': 'get',
                'url': 'https://weather.uni-freiburg.de/',
            }
        }

    def process_one_value(self, name, data):
        number_regex = '[\\-+]?[0-9]+(?:[\\.,][0-9]+)?(?:[eE][\\-+]?[0-9]+)?' # Need to update this in Doberman some time (?)
        vals = re.findall(f'class="[\w\s]*meteo-val[\w\s]*"[\s\w"=]*\>\s*(?:\<[\s\w"=]*\>)*\s*(?:[^\<\>]*)\s*(?:\</[\s\w"=]*\>)*\s*(?:\<[\s\w"=]*\>)*\s*(?P<value>{number_regex})', data)
        return [float(x.replace(',', '.')) for x in vals]
