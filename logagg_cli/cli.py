import sys
from os.path import expanduser

import ujson as json
from tabulate import tabulate
import requests
from diskdict import DiskDict
from logagg_utils import ensure_dir
from deeputil import AttrDict
from structlog.dev import ConsoleRenderer


def prGreen(txt): print("\033[92m {}\033[00m" .format(txt))

def prRed(err): print("\033[91m {}\033[00m" .format(err))

def prYellow(txt): print("\033[93m {}\033[00m" .format(txt))


class LogaggCli():
    '''
    Command line interface for logagg
    '''
    
    MASTER_PING_URL = 'http://{host}:{port}/logagg/v1/ping?key={key}&secret={secret}'
    MASTER_ADD_NSQ_URL = 'http://{host}:{port}/logagg/v1/add_nsq?nsqd_tcp_address={nsqd_tcp_address}&nsqd_http_address={nsqd_http_address}&key={key}&secret={secret}'
    MASTER_GET_NSQ_URL = 'http://{host}:{port}/logagg/v1/get_nsq?key={key}&secret={secret}'
    GET_TOPIC_URL = 'http://{host}:{port}/logagg/v1/get_topics'
    GET_TOPIC_INFO_URL = 'http://{host}:{port}/logagg/v1/get_topic_info?topic_name={topic_name}'
    GET_COMPONENT_URL = 'http://{host}:{port}/logagg/v1/get_components?topic_name={topic_name}'
    TAIL_LOGS_URL = 'http://{host}:{port}/logagg/v1/tail_logs?topic_name={topic_name}'
    COLLECTOR_ADD_FILE_URL = 'http://{host}:{port}/logagg/v1/collector_add_file?topic_name={topic_name}&collector_host={collector_host}&collector_port={collector_port}&fpath="{fpath}"&formatter="{formatter}"'
    COLLECTOR_REMOVE_FILE_URL = 'http://{host}:{port}/logagg/v1/collector_remove_file?topic_name={topic_name}&collector_host={collector_host}&collector_port={collector_port}&fpath="{fpath}"'

    def __init__(self):
        self.data_path = ensure_dir(expanduser('~/.logagg'))
        self.state = DiskDict(self.data_path)
        self._init_state()

    def _init_state(self):
        '''
        Initialize default values for stored state
        '''
        if not self.state['master']:
            self.state['master'] = dict()
            self.state.flush()
        if not self.state['default_topic']:
            self.state['default_topic'] = dict()
            self.state.flush()

    def ensure_master(self):
        '''
        Check if Master details are present
        '''
        if not self.state['master']:
            err_msg = 'No master details stored locally'
            prRed(err_msg)
            sys.exit(1)
        else:
            return AttrDict(self.state['master'])

    def request_master_url(self, url):
        '''
        Request mater urls and return response
        '''
        try:
            response =  requests.get(url)
            response = json.loads(response.content.decode('utf-8'))
            return response

        except requests.exceptions.ConnectionError:
            err_msg = 'Could not reach master, url: {}'.format(url)
            prRed(err_msg)
            sys.exit(1)

    def clear(self):
        '''
        Delete all saved data
        '''
        self.state['master'] = dict()
        self.state['default_topic'] = dict()
        self.state.flush()

    def store_master(self, host, port, auth):
        '''
        Add master details to state file 
        '''
        ping_url = self.MASTER_PING_URL.format(host=host, port=port, key=auth.key, secret=auth.secret)
        ping_result = self.request_master_url(ping_url)

        if ping_result.get('result', {}).get('success', {}):
            if ping_result.get('result', {}).get('details', {}) == 'Authentication passed':
                master_details = {'host': host, 'port': port, 'key': auth.key, 'secret': auth.secret, 'admin': True}
                self.state['master'] = master_details
                self.state.flush()
                prGreen('Added master with admin permission')
            elif ping_result.get('result', {}).get('details', {}) == 'Authentication failed' and not auth.key and not auth.secret:
                master_details = {'host': host, 'port': port, 'key': auth.key, 'secret': auth.secret, 'admin': False}
                self.state['master'] = master_details
                self.state.flush()
                prYellow('Added master with non-admin permission')
            else:
                err_msg = ping_result.get('result', {}).get('details', {})
                prRed(err_msg)
                sys.exit(1)

    def list_master(self):
        '''
        Show Master details
        '''
        master = self.ensure_master()
        headers = ['HOST', 'PORT', 'ADMIN']

        data = [[master.host, master.port, str(master.admin)]]
        print(tabulate(data, headers=headers))

    def add_nsq(self,  nsqd_tcp_address, nsqd_http_address):
        '''
        Add nsq details to master
        '''
        master = self.ensure_master()

        if not master.admin:
            err_msg = 'Requires admin permissions to master'
            prRed(err_msg)
            sys.exit(1)

        add_nsq_url = self.MASTER_ADD_NSQ_URL.format(host=master.host,
                                                        port=master.port,
                                                        nsqd_tcp_address=nsqd_tcp_address,
                                                        nsqd_http_address=nsqd_http_address,
                                                        key=master.key,
                                                        secret=master.secret)

        add_nsq_result = self.request_master_url(add_nsq_url)

        if add_nsq_result.get('result', {}).get('success', {}):
            prGreen(add_nsq_result.get('result', {}).get('details', {}))
        else:
            err_msg =  add_nsq_result.get('result', {}).get('details', {})
            prRed(err_msg)
            sys.exit(1)

    def list_nsq(self):
        '''
        List nsq details of master
        '''
        master = self.ensure_master()

        if not master.admin:
            err_msg = 'Requires admin permissions to master'
            prRed(err_msg)
            sys.exit(1)

        get_nsq_url = self.MASTER_GET_NSQ_URL.format(host=master.host,
                                                     port=master.port,
                                                     key=master.key,
                                                     secret=master.secret)

        get_nsq_result = self.request_master_url(get_nsq_url)
    
        if get_nsq_result.get('result', {}).get('success', {}):
            nsq_details = get_nsq_result.get('result', {}).get('nsq_list', {})
            headers = ['Nsqd TCP address', 'Nsqd HTTP address', 'Nsq depth limit', 'Nsq API address']
            data = list()
            for nsq in nsq_details: data.append(list(nsq.values())) 
            print(tabulate(data, headers=headers))
        else:
            err_msg = get_nsq_result.get('result', {}).get('details', {})
            prRed(err_msg)
            sys.exit(1)

    def list_topic(self):
        '''
        List all the topics in master
        '''
        master = self.ensure_master()

        # Get list of all topics from master
        list_topic_url = self.GET_TOPIC_URL.format(host=master.host,
                                                   port=master.port)
        list_topic_result =  self.request_master_url(list_topic_url)
        topic_list = list_topic_result.get('result', {})

        master = self.state['master']
        master_admin = master.get('admin')

        for topic in topic_list:
            if topic['topic_name'] == self.state['default_topic'].get('topic_name'):
                topic['default_topic'] = True
            else:
                topic['default_topic'] = False
            if not master_admin:
                topic.pop('nsqd_tcp_address')
                topic.pop('nsqd_http_address')
                topic.pop('nsq_depth_limit')
                topic.pop('nsq_api_address')
                topic.pop('heartbeat_topic')
                topic.pop('logs_topic')

        headers = list()

        if not master_admin:
            headers = ['Topic-name',
                    'Default topic']
        else:
            headers = ['Topic-name',
                    'Nsqd TCP address',
                    'Nsqd HTTP address',
                    'NSQ max depth',
                    'Nsq API address',
                    'Heartbeat topic',
                    'Logs topic',
                    'Default topic']

        data =  list()
        for c in topic_list: data.append(list(c.values()))
        print(tabulate(data, headers=headers))

    def ensure_topic_info(self, topic_name):
        '''
        Ensure topic info is saved locally
        '''
        master = self.ensure_master()

        # Get list of all topics from master
        list_topic_url = self.GET_TOPIC_URL.format(host=master.host,
                                                   port=master.port)
        list_topic_result =  self.request_master_url(list_topic_url)
        topic_list = list_topic_result.get('result', [])

        for topic in topic_list:
            if topic['topic_name'] == topic_name:
                return topic
        err_msg = 'No topic found, topic-name: {topic_name}'.format(topic_name=topic_name)
        prRed(err_msg)
        sys.exit(1)

    def use_topic(self, topic_name):
        '''
        Make a topic usable by default
        '''
        topic = self.ensure_topic_info(topic_name)

        self.state['default_topic'] = topic
        self.state.flush()
        prGreen('Switched to default: {}'.format(topic_name))

    def list_collectors(self):
        '''
        List collectors in an existing topic
        '''
        master = self.ensure_master()

        if not self.state['default_topic']:
            err_msg = 'No default topic'
            prRed(err_msg)
            sys.exit(1)
        else:
            topic_name = self.state['default_topic']['topic_name']

            get_components_url = self.GET_COMPONENT_URL.format(host=master.host,
                                                                port=master.port,
                                                                topic_name=topic_name)

            get_components_result = self.request_master_url(get_components_url)

            if get_components_result.get('result', {}).get('success', {}): 
                components_info = get_components_result.get('result', {}).get('components_info')

                headers = ['Namespace',
                        'Host',
                        'Port',
                        'Topic name',
                        'files tracked',
                        'Heartbeat number',
                        'timestamp',]

                data =  list()
                for c in components_info:
                    if c.get('namespace') == 'collector':
                        data.append([c.get('namespace'),
                                     c.get('host'),
                                     c.get('port'),
                                     c.get('topic_name'),
                                     c.get('files_tracked'),
                                     c.get('heartbeat_number'),
                                     c.get('timestamp')]
                                     )
                print(tabulate(data, headers=headers))

            else:
                # Print result
                msg = get_components_result.get('result', {}).get('details', {})
                prRed(msg)
                sys.exit(1)

    def tail(self, pretty):
        '''
        Tail the logs of a topic
        '''
        master = self.ensure_master()
        if not self.state['default_topic']:
            err_msg = 'No default topic'
            prRed(err_msg)
            sys.exit(1)
        else:
            topic_name = self.state['default_topic']['topic_name']

            tail_logs_url = self.TAIL_LOGS_URL.format(host=master.host,
                                                        port=master.port,
                                                        topic_name=topic_name)

            try:
                session = requests.session()
                resp = session.get(tail_logs_url, stream=True)
                c = ConsoleRenderer()
                for line in resp.iter_lines():
                    log = dict()
                    try:
                        result = json.loads(line.decode('utf-8'))
                        result = result.get('result')
                        if result: log = json.loads(result)
                        else: continue
                    except ValueError:
                        print(Exception('ValueError log:{}'.format(result)))
                        continue
                    if pretty:
                        print(c(None, None, log))
                    else:
                        print(log)
            except requests.exceptions.ConnectionError:
                err_msg = 'Cannot request master'
                prRed(err_msg)
                sys.exit(1)
            except Exception as e:
                if resp: resp.close()
                raise e
                sys.exit(1)

    def collector_add_file(self, collector_host, collector_port, fpath, formatter):
        '''
        Add file to collector
        '''
        master = self.ensure_master()

        if not self.state['default_topic']:
            err_msg = 'No default topic'
            prRed(err_msg)
            sys.exit(1)
        else:
            topic_name = self.state['default_topic']['topic_name']

            add_file_url = self.COLLECTOR_ADD_FILE_URL.format(host=master.host,
                                                                port=master.port,
                                                                topic_name=topic_name,
                                                                collector_host=collector_host,
                                                                collector_port=collector_port,
                                                                fpath=fpath,
                                                                formatter=formatter)

            add_file_result = self.request_master_url(add_file_url)

            if add_file_result.get('result', {}).get('success', {}): 
                new_fpaths_list = list()
                for fpath in add_file_result.get('result', {})['fpaths']: new_fpaths_list.append([fpath['fpath']])
                headers = ['File paths']
                data = list()
                #print result
                print(tabulate(new_fpaths_list, headers=headers))

            else:
                # Print result
                msg = get_components_result.get('result', {}).get('details', {})
                prRed(msg)
                sys.exit(1)

    def collector_remove_file(self, collector_host, collector_port, fpath):
        '''
        Remove file-path from collector
        '''
        master = self.ensure_master()

        if not self.state['default_topic']:
            err_msg = 'No default topic'
            prRed(err_msg)
            sys.exit(1)
        else:
            topic_name = self.state['default_topic']['topic_name']

            remove_file_url = self.COLLECTOR_REMOVE_FILE_URL.format(host=master.host,
                                                                port=master.port,
                                                                topic_name=topic_name,
                                                                collector_host=collector_host,
                                                                collector_port=collector_port,
                                                                fpath=fpath)

            remove_file_result = self.request_master_url(remove_file_url)

            if remove_file_result.get('result', {}).get('success', {}): 
                new_fpaths_list = list()
                for fpath in remove_file_result.get('result', {})['fpaths']: new_fpaths_list.append([fpath['fpath']])
                headers = ['File paths']
                data = list()
                #print result
                print(tabulate(new_fpaths_list, headers=headers))

            else:
                # Print result
                msg = remove_file_result
                prRed(msg)
                sys.exit(1)
