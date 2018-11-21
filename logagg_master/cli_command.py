
from basescript import BaseScript
from deeputil import AttrDict

from .cli import LogaggCli

class LogaggCliCommand(BaseScript):
    DESC = 'Logagg Master service and Command line tool'

    def __init__(self):
        super().__init__()

    def _parse_auth_args(self):
        auth_dict = dict()

        if self.args.auth:
            args = self.args.auth.split(':')

            for a in args:
                key, value = a.split('=')
                auth_dict[key] = value
            
            return AttrDict(auth_dict)
        else:
            auth_dict = {'key': None, 'secret': None}
            return AttrDict(auth_dict)
            

    def store_master(self):
        try:
            auth = self._parse_auth_args()
        except:
            raise Exception('Invalid Argument', arg=self.args.auth)

        LogaggCli().store_master(self.args.host, self.args.port, auth)

    def list_master(self):
        LogaggCli().list_master()

    def add_nsq(self):
        LogaggCli().add_nsq(self.args.nsqd_tcp_address, self.args.nsqd_http_address)

    def list_nsq(self):
        LogaggCli().list_nsq()

    def list_topic(self):
        LogaggCli().list_topic()

    def use_topic(self):
        LogaggCli().use_topic(self.args.topic_name)

    def list_collectors(self):
        LogaggCli().list_collectors()

    def collector_add_file(self):
        LogaggCli().collector_add_file(self.args.collector_host,
                self.args.collector_port,
                self.args.fpath,
                self.args.formatter)

    def collector_remove_file(self):
        LogaggCli().collector_remove_file(self.args.collector_host,
                self.args.collector_port,
                self.args.fpath)

    def tail(self):
        LogaggCli().tail(self.args.pretty)

    def clear(self):
        LogaggCli().clear()

    def define_subcommands(self, subcommands):
        super(LogaggCliCommand, self).define_subcommands(subcommands)

        # clear
        clear_cmd = subcommands.add_parser('clear',
                help='Clear all saved data')
        clear_cmd.set_defaults(func=self.clear)

        # master
        master_cmd = subcommands.add_parser('master',
                help='Logagg-master details')
        master_cmd_subparser = master_cmd.add_subparsers()
        # master store
        master_cmd_store = master_cmd_subparser.add_parser('store',
                help='Store logagg-master details')
        master_cmd_store.set_defaults(func=self.store_master)
        master_cmd_store.add_argument(
                '--port', '-p', required=True,
                help='Port on which logagg master service is running on')
        master_cmd_store.add_argument(
                '--host', '-i', required=True,
                help='Hostname on which logagg master service is running on')
        master_cmd_store.add_argument(
                '--auth', '-a',
                help= 'Service auth details, format: <key=xyz:secret=xxxx>')
        # master list
        master_cmd_list = master_cmd_subparser.add_parser('list',
                help='Print logagg-master details')
        master_cmd_list.set_defaults(func=self.list_master)
        # master nsq
        master_cmd_nsq_subparser = master_cmd_subparser.add_parser('nsq',
                help='NSQ for logagg-master')
        # master nsq add
        master_cmd_nsq_subparser_add = master_cmd_nsq_subparser.add_subparsers()
        master_cmd_nsq_subparser_add_parser = master_cmd_nsq_subparser_add.add_parser('add',
                help='Add NSQ details for logagg-master')
        master_cmd_nsq_subparser_add_parser.set_defaults(func=self.add_nsq)
        master_cmd_nsq_subparser_add_parser.add_argument(
                '--nsqd-tcp-address', '-t', required=True,
                help='Nsqd tcp address, format: <localhost:4150>')
        master_cmd_nsq_subparser_add_parser.add_argument(
                '--nsqd-http-address', '-w', required=True,
                help='Nsqd http address, format: <localhost:4151>')
        # master nsq list
        master_cmd_nsq_subparser_list_parser = master_cmd_nsq_subparser_add.add_parser('list',
                help='Print NSQ details for logagg-master')
        master_cmd_nsq_subparser_list_parser.set_defaults(func=self.list_nsq)

        # topic
        topic_cmd_parser = subcommands.add_parser('topic',
                help='Operations on topics in master')
        topic_cmd_subparser = topic_cmd_parser.add_subparsers()
        # topic list
        topic_cmd_list = topic_cmd_subparser.add_parser('list',
                help='List all the topics in master')
        topic_cmd_list.set_defaults(func=self.list_topic)
        # topic use
        topic_cmd_use = topic_cmd_subparser.add_parser('use',
                help='Use an existing topic')
        topic_cmd_use.set_defaults(func=self.use_topic)
        topic_cmd_use.add_argument(
                '--topic-name', '-n', required=True,
                help='Name of the topic')
        # topic tail
        topic_cmd_tail = topic_cmd_subparser.add_parser('tail',
                help='Tail logs in topic')
        topic_cmd_tail.set_defaults(func=self.tail)
        topic_cmd_tail.add_argument(
                '--pretty', '-p',
                action='store_true',
                help='Print logs in pretty format')
        # topic collector
        topic_cmd_collector = topic_cmd_subparser.add_parser('collector',
                help='Operations on topic collectors')
        topic_cmd_collector_subparser = topic_cmd_collector.add_subparsers()
        # topic collector list
        topic_cmd_collector_list = topic_cmd_collector_subparser.add_parser('list',
                 help='List all collectors')
        topic_cmd_collector_list.set_defaults(func=self.list_collectors)
        # topic collector add-file
        topic_cmd_collector_add_file = topic_cmd_collector_subparser.add_parser('add-file',
                help='Add file paths to collectors')
        topic_cmd_collector_add_file.set_defaults(func=self.collector_add_file)
        topic_cmd_collector_add_file.add_argument(
                '--collector-host', '-c',
                required=True,
                help='Host on which the collector is running')
        topic_cmd_collector_add_file.add_argument(
                '--collector-port', '-p',
                required=True,
                help='Port on which collector service is running')
        topic_cmd_collector_add_file.add_argument(
                '--fpath', '-f',
                help='File path of the log-file on the node where collector is running')
        topic_cmd_collector_add_file.add_argument(
                '--formatter', '-b',
                help='Formatter to use for the log-file')
        # topic collector remove-file
        topic_cmd_collector_remove_file = topic_cmd_collector_subparser.add_parser('remove-file',
                help='Remove file-path from collectors')
        topic_cmd_collector_remove_file.set_defaults(func=self.collector_remove_file)
        topic_cmd_collector_remove_file.add_argument(
                '--collector-host', '-c',
                required=True,
                help='Host on which the collector is running')
        topic_cmd_collector_remove_file.add_argument(
                '--collector-port', '-p',
                required=True,
                help='Port on which collector service is running')
        topic_cmd_collector_remove_file.add_argument(
                '--fpath', '-f',
                help='File path of the log-file on the node where collector is running')


def main():
    LogaggCliCommand().start()

if __name__ == '__main__':
    main()
