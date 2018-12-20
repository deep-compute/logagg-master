import uuid
from typing import Generator
import time

import ujson as json
from kwikapi import Request, BaseProtocol
import tornado
import pymongo
from pymongo import MongoClient
from deeputil import keeprunning, AttrDict
import requests
from logagg_utils import log_exception, start_daemon_thread

class MasterService():
    '''
    Logagg master API
    '''
    NSQ_API_URL = 'http://{nsq_api_address}/tail?nsqd_tcp_address={nsqd_tcp_address}&nsqd_http_address={nsqd_http_address}&topic={topic}&empty_lines={empty_lines}'
    COLLECTOR_ADD_FILE_URL = 'http://{collector_address}/collector/v1/add_file?fpath={fpath}&formatter={formatter}'
    COLLECTOR_REMOVE_FILE_URL = 'http://{collector_address}/collector/v1/remove_file?fpath="{fpath}"'
    COLLECTOR_STOP_URL = 'http://{collector_address}/collector/v1/stop'
    NSQ_DEPTH_LIMIT = 1000000

    def __init__(self, master, log):

        self.master = master
        self.log = log

    def ping(self, key:str, secret:str) -> dict:
        '''
        Sample url:
        'http://localhost:1088/logagg/v1/ping?key=xyz&secret=xxxx'
        '''

        if key == self.master.auth.key and secret == self.master.auth.secret:
            return {'success': True, 'details': 'Authentication passed'}
        else:
            return {'success': True, 'details': 'Authentication failed'}

        return {'success': False, 'details': 'Authentication failed'}

    def add_nsq(self, nsqd_tcp_address:str, nsqd_http_address:str, key:str, secret:str) -> dict:
        '''
        Insert nsq details into master
        Sample url:
        'http://localhost:1088/logagg/v1/add_nsq?nsqd_tcp_address="<hostname>:4150"&nsqd_http_address="<hostname>:4151"&key=xyz&secret=xxxx'
        '''

        if key == self.master.auth.key and secret == self.master.auth.secret:
            nsq_api = dict()
            random_nsq_api = self.master.nsq_api_collection.aggregate([{'$sample': {'size': 1}}])

            for n in random_nsq_api:
                nsq_api = n

            if not nsq_api:
                return {'success': False, 'details': 'No nsq_api in master to assign to NSQ'}

            details = {'nsqd_tcp_address': nsqd_tcp_address,
                        'nsqd_http_address': nsqd_http_address,
                        'nsq_depth_limit': self.NSQ_DEPTH_LIMIT,
                        'nsq_api_address': nsq_api['host']+':'+str(nsq_api['port'])}

            try:
                object_id = self.master.nsq_collection.insert_one(details).inserted_id

            except pymongo.errors.DuplicateKeyError as dke:
                return {'details': 'Duplicate nsq details', 'success': False}

            return {'success': True, 'details': 'Added NSQ details'}

        else:

            return {'success': False, 'details': 'Authentication failed'}

    def get_nsq(self, key:str, secret:str) -> list:
        '''
        Get nsq details
        Sample url:
        'http://localhost:1088/logagg/v1/get_nsq?key=xyz&secret=xxxx'
        '''

        if key == self.master.auth.key and secret == self.master.auth.secret:
            nsq = self.master.nsq_collection.find()
            nsq_list = list()
            for n in nsq:
                n.pop('_id')
                nsq_list.append(n)
            return {'success': True, 'nsq_list':nsq_list}
        else:
            return {'success': False, 'details': 'Authentication failed'}

    def register_nsq_api(self, key:str, secret:str, host:str, port:str) -> dict:
        '''
        Validate auth details and store details of component in master
        Sample url:
        'http://localhost:1088/logagg/v1/register_nsq_api?key=xyz&secret=xxxx&host=localhost&port=1077'
        '''

        if key == self.master.auth.key and secret == self.master.auth.secret:

            details = {'host': host,
                'port': port}

            try:
                object_id = self.master.nsq_api_collection.insert_one(details).inserted_id

            except pymongo.errors.DuplicateKeyError as dke:
                return {'details': 'Duplicate nsq_api details', 'success': True}

            return {'success': True, 'details': 'Added nsq_api details'}

        else:

            return {'success': False, 'details': 'Authentication failed'}
   
    def _create_topic(self, topic_name):
        '''
        Create topic in master database
        '''

        random_nsq = self.master.nsq_collection.aggregate([{'$sample': {'size': 1}}])
        nsq = dict()

        for n in random_nsq:
            nsq = n

        if not nsq:
            return {'success': False, 'details': 'No NSQ in master to assign to topic'}

        topic_info = {'topic_name': topic_name,
                        'nsqd_tcp_address': nsq['nsqd_tcp_address'],
                        'nsqd_http_address': nsq['nsqd_http_address'],
                        'nsq_depth_limit': nsq['nsq_depth_limit'],
                        'nsq_api_address': nsq['nsq_api_address'],
                        'heartbeat_topic': topic_name+'_heartbeat#ephemeral',
                        'logs_topic': topic_name+'_logs'}
        try:
            object_id = self.master.topic_collection.insert_one(topic_info).inserted_id
            return {'success': True, 'topic_name': topic_name}

        except pymongo.errors.DuplicateKeyError as dke:
            return {'success': True, 'topic_name': topic_name}

    def get_topics(self) -> list:
        '''
        Get topic information
        Sample url:
        'http://localhost:1088/logagg/v1/get_topics'
        '''

        topics = self.master.topic_collection.find()

        topic_list = list()
        for c in topics:
            del c['_id']
            topic_list.append(c)

        return topic_list

    def get_topic_info(self, topic_name:str) -> dict:
        '''
        Get details of a particular topic
        Sample url:
        'http://localhost:1088/logagg/v1/get_topic_info?topic_name=logagg'
        '''

        topic = self.master.topic_collection.find_one({'topic_name': topic_name})
        if not topic:
            return {'success': False, 'details': 'Topic name not found'}
        else:
            del topic['_id']
            return {'success': True, 'topic_info': topic}

    def _delete_topic(self, topic_name:str):
        '''
        Delete topic from master database
        '''

        topic = self.master.topic_collection.find_one({'topic_name': topic_name})
        if not topic: return {'success': False, 'details': 'Topic name not found'}

        components = self.master.component_collection.find({'topic_name': topic_name})
        collectors = [c for c in components if c['namespace'] == 'collector']

        for collector in collectors:
            collector_address = collector['host'] + ':' + collector['port']
            stop_collector_url = self.COLLECTOR_STOP_URL.format(collector_address=collector_address)
            try:
                stop_collector_result = requests.get(stop_collector_url).content
            except requests.exceptions.ConnectionError:
                # Beacause collector terminate before sending any response 
                delete_collectors = self.master.component_collection.delete_many({'topic_name': topic_name,
                                                                            'namespace': 'collector'})
        delete_topic = self.master.topic_collection.delete_one({'topic_name': topic_name})

        return {'success': True, 'collectors_stopped': delete_topic.deleted_count}

    def register_component(self, namespace:str, topic_name:str, host:str, port:str) -> dict:
        '''
        Validate auth details and store details of component in database
        Sample url:
        'http://localhost:1088/logagg/v1/register_component?namespace=master&topic_name=logagg&host=78.47.113.210&port=1088'
        '''
        
        # find for topic or create one
        topic = self.master.topic_collection.find_one({'topic_name': topic_name})
        if not topic: create_topic_result = self._create_topic(topic_name)

        if topic or create_topic_result.get('success'):
            component = {'namespace':namespace,
                         'host':host,
                         'port':str(port),
                         'topic_name':topic_name}

            component_info = AttrDict(component)
            try:
                topic_name= component_info.topic_name
                namespace = component_info.namespace
                host = component_info.host
                port = component_info.port
                self.master.component_collection.update_one({'topic_name': topic_name,
                                                                'namespace': namespace,
                                                                'host': host,
                                                                'port': port},
                                                            {'$set': component},
                                                            upsert=True)
                return {'success': True}
            except pymongo.errors.DuplicateKeyError as dke:
                return {'success': True, 'details': 'Duplicate component details'}

        else: return create_topic_result

    def get_components(self, topic_name:str) -> dict:
        '''
        Get all components in a topic
        Sample url:
        'http://localhost:1088/logagg/v1/get_components?topic_name=logagg'
        '''
        components_info = list()
        topic =  self.master.topic_collection.find_one({'topic_name': topic_name})
        if not topic:
            return {'success': False, 'details': 'Topic not found'}

        for c in self.master.component_collection.find({'topic_name': topic_name}):
            del c['_id']
            components_info.append(c)
        return {'success': True, 'components_info': components_info}

    def collector_add_file(self, topic_name:str,
                            collector_host:str,
                            collector_port:str,
                            fpath:str,
                            formatter:str) -> dict:
        '''
        Add files to collectors
        Sample url: 'http://localhost:1088/logagg/v1/collector_add_file?namespace=master&
                     topic_name=logagg&collector_host=localhost&collector_port=1088&
                     fpath="/var/log/serverstats.log"&formatter="logagg_collector.formatters.docker_file_log_driver"'
        '''
        topic =  self.master.topic_collection.find_one({'topic_name': topic_name})
        if not topic: return {'success': False, 'details': 'Topic not found'}

        collector_port = str(collector_port)
        collector = self.master.component_collection.find_one({'topic_name': topic_name,
                                                                'host': collector_host,
                                                                'port': collector_port,
                                                                'namespace': 'collector'})
        if not collector: return {'success': False, 'details': 'Collector not found'}
        else:
            collector_address = collector_host + ':' + collector_port
            add_file_url = self.COLLECTOR_ADD_FILE_URL.format(collector_address=collector_address,
                                                                fpath=fpath,
                                                                formatter=formatter)
            try:
                add_file_result = requests.get(add_file_url).content
                add_file_result = json.loads(add_file_result.decode('utf-8'))
            except requests.exceptions.ConnectionError:
                return {'success': False, 'details': 'Could not reach collector'}
            return {'success': True, 'fpaths': add_file_result.get('result')}

    def collector_remove_file(self, topic_name:str,
                            collector_host:str,
                            collector_port:str,
                            fpath:str) -> dict:
        '''
        remove file-path from collectors
        Sample url: 'http://localhost:1088/logagg/v1/collector_remove_file?namespace=master&
                     topic_name=logagg&collector_host=localhost&collector_port=1088&
                     fpath="/var/log/serverstats.log"'
        '''
        topic =  self.master.topic_collection.find_one({'topic_name': topic_name})
        if not topic:
            return {'success': False, 'details': 'Topic not found'}

        collector_port = str(collector_port)
        collector = self.master.component_collection.find_one({'topic_name': topic_name,
                                                                'host': collector_host,
                                                                'port': collector_port,
                                                                'namespace': 'collector'})
        if not collector:
            return {'success': False, 'details': 'Collector not found'}
        else:
            collector_address = collector_host + ':' + collector_port
            remove_file_url = self.COLLECTOR_REMOVE_FILE_URL.format(collector_address=collector_address,
                                                                fpath=fpath)
            try:
                remove_file_result = requests.get(remove_file_url).content
                remove_file_result = json.loads(remove_file_result.decode('utf-8'))
            except requests.exceptions.ConnectionError:
                return {'success': False, 'details': 'Could not reach collector'}
            return {'success': True, 'fpaths': remove_file_result.get('result')}

    def tail_logs(self, req:Request, topic_name:str) -> Generator:
        '''
        Sample url:
        'http://localhost:1088/logagg/v1/tail_logs?topic_name=logagg'
        '''
        topic =  self.master.topic_collection.find_one({'topic_name': topic_name})
        if not topic: return {'success': False, 'details': 'Topic not found'}

        nsq_api_address = topic['nsq_api_address']
        log_topic = topic['logs_topic']
        nsqd_tcp_address = topic['nsqd_tcp_address']
        nsqd_http_address = topic['nsqd_http_address']
        url = self.NSQ_API_URL.format(nsq_api_address=nsq_api_address,
                                        nsqd_tcp_address=nsqd_tcp_address,
                                        nsqd_http_address=nsqd_http_address,
                                        topic=log_topic,
                                        empty_lines='yes')
        s = requests.session()
        try:
            resp = s.get(url, stream=True)
        except requests.exceptions.ConnectionError:
            self.log.error('cannot_request_nsq_api', url=url)
            return  {'success': False, 'details': 'Cannot request nsq api'}
        start = time.time()
        log_list = list()

        for log in resp.iter_lines():
            if req._request.connection.stream.closed():
                self.log.debug('stream_closed')
                resp.close()
                break
            if log:
                log_list.append(log.decode('utf-8') + '\n')
            else:
                 yield ''
            if time.time() - start >= 1:
                for l in log_list: yield l
                log_list = []
                start = time.time()


class Master():
    '''
    Logagg master class
    '''
    NSQ_API_URL = 'http://{nsq_api_address}/tail?nsqd_tcp_address={nsqd_tcp_address}&nsqd_http_address={nsqd_http_address}&topic={topic}'
    SERVER_SELECTION_TIMEOUT = 500  # MongoDB server selection timeout
    NAMESPACE = 'master'
    UPDATE_COMPONENTS_INTERVAL = 30

    def __init__(self, host, port, mongodb, auth, log):

        self.host = host
        self.port = port
        self.auth = auth

        self.log = log
        self.mongodb = mongodb
        self.db_client = self._ensure_db_connection()
        self._init_mongo_collections()
        self.update_component_thread = start_daemon_thread(self.update_components)
        self.update_topic_components_threads = dict()

    def _init_mongo_collections(self):
        # Collection for nsq details
        self.nsq_collection = self.db_client['nsq']
        self.nsq_collection.create_index([
            ('nsqd_tcp_address', pymongo.ASCENDING),
            ('nsqd_http_address', pymongo.ASCENDING)],
            unique=True)

        # Collection for nsq apis
        self.nsq_api_collection = self.db_client['nsq_api']
        self.nsq_api_collection.create_index([
            ('host', pymongo.ASCENDING),
            ('port', pymongo.ASCENDING)],
            unique=True)

        # Collection for components
        self.component_collection = self.db_client['components']
        self.component_collection.create_index([
            ('host', pymongo.ASCENDING),
            ('port', pymongo.ASCENDING),],
            unique=True)
        #FIXME: does not serve it's purpose of expiring records
        #self.component_collection.ensure_index('timestamp', expireAfterSeconds=60)

        # Collection for topic info
        self.topic_collection = self.db_client['topic']
        self.topic_collection.create_index([
             ('topic_name', pymongo.ASCENDING)],
             unique=True)

    def _ensure_db_connection(self):
        url = 'mongodb://{}:{}@{}:{}'.format(self.mongodb.user,
                self.mongodb.passwd,
                self.mongodb.host,
                self.mongodb.port)

        client = MongoClient(url, serverSelectionTimeoutMS=self.SERVER_SELECTION_TIMEOUT)
        self.log.info('mongodb_server_connection_established', db=dict(self.mongodb))
        db_client = client[self.mongodb.name]

        return db_client

    @keeprunning(UPDATE_COMPONENTS_INTERVAL, on_error=log_exception)
    def _update_topic_components(self, topic_name):
        '''
        Starts a deamon thread for reading from heartbeat topic and updarting info in database
        '''
        topic_info = self.topic_collection.find_one({'topic_name': topic_name})
        topic = topic_info['heartbeat_topic']
        nsqd_tcp_address = topic_info['nsqd_tcp_address']
        nsqd_http_address = topic_info['nsqd_http_address']
        nsq_api_address = topic_info['nsq_api_address']

        url = self.NSQ_API_URL.format(nsq_api_address=nsq_api_address,
                                        nsqd_tcp_address=nsqd_tcp_address,
                                        nsqd_http_address=nsqd_http_address,
                                        topic=topic,
                                        empty_lines='no')
        try:
            self.log.info("updating_components", topic=topic_name)
            resp = requests.get(url, stream=True)
            start_read_heartbeat = time.time()
            for heartbeat in resp.iter_lines():
                heartbeat = AttrDict(json.loads(heartbeat.decode('utf-8')))
                host = heartbeat.host
                port = heartbeat.port
                self.component_collection.update_one({'host': host, 'port': port},
                                                     {'$set': heartbeat},
                                                     upsert=True)

        except requests.exceptions.ConnectionError:
            self.log.warn('cannot_request_nsq_api___will_try_again', url=url)

        except KeyboardInterrupt:
            if resp: resp.close()
            sys.exit(0)
        time.sleep(self.UPDATE_COMPONENTS_INTERVAL)

    @keeprunning(UPDATE_COMPONENTS_INTERVAL, on_error=log_exception)
    def update_components(self):
        '''
        Reads heartbeat and updates components
        '''
        # List of topic names
        topic_list = list()
        for c in self.topic_collection.find(): topic_list.append(c['topic_name'])

        for topic_name in topic_list:
            if topic_name not in self.update_topic_components_threads:
                update_topic_components_thread = start_daemon_thread(self._update_topic_components, (topic_name,))
                self.update_topic_components_threads[topic_name] = update_topic_components_thread

        time.sleep(self.UPDATE_COMPONENTS_INTERVAL)
