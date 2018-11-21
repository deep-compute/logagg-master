import os
import socket

from basescript import BaseScript
from deeputil import AttrDict
import tornado.ioloop
import tornado.web
from kwikapi.tornado import RequestHandler
from kwikapi import API

from .master_service import MasterService, Master
from .exceptions import InvalidArgument

class LogaggMasterCommand(BaseScript):
    DESC = 'Logagg Master service and Command line tool'

    def run(self):

        port = self.args.port
        host = self.args.host 

        auth = AttrDict()
        try:
            m = self.args.auth.split(':')
            # So that order of keys is not a factor
            for a in m:
                a = a.split('=')
                if a[0] == 'key': auth.key = a[-1]
                elif a[0] == 'secret': auth.secret = a[-1]
                else: raise ValueError

        except ValueError:
            raise InvalidArgument(self.args.auth)

        mongodb = AttrDict()
        try:
            m = self.args.mongodb.split(':')
            for a in m:
                a = a.split('=')
                if a[0] == 'host': mongodb.host = a[-1]
                elif a[0] == 'port': mongodb.port = a[-1]
                elif a[0] == 'user': mongodb.user = a[-1]
                elif a[0] == 'passwd': mongodb.passwd = a[-1]
                elif a[0] == 'db': mongodb.name = a[-1]
                else: raise ValueError

        except ValueError:
            raise InvalidArgument(self.args.mongodb)

        # Create LogaggService object
        ls = Master(host,
                port,
                mongodb,
                auth,
                self.log)

        master_api = MasterService(ls, self.log)
        api = API()
        api.register(master_api, 'v1')

        app = tornado.web.Application([
            (r'^/logagg/.*', RequestHandler, dict(api=api)),
                ])

        app.listen(self.args.port)
        tornado.ioloop.IOLoop.current().start()

    def define_subcommands(self, subcommands):
        super(LogaggMasterCommand, self).define_subcommands(subcommands)

        master_cmd = subcommands.add_parser('runserver',
                help='Run logagg master service')

        master_cmd.set_defaults(func=self.run)

        master_cmd.add_argument(
                '--port', '-p', default=1088,
                help='Port to run logagg master service on, default: %(default)s')

        master_cmd.add_argument(
                '--host', '-i', default=socket.gethostname(),
                help='Hostname of this service for other components to contact to, default: %(default)s')

        master_cmd.add_argument(
                '--auth', '-a', required=True,
                help= 'Service auth details to grant access to components, format: <key=xyz:secret=xxxx>')

        master_cmd.add_argument(
                '--mongodb', '-d', required=True,
                help= 'Database details, format: <host=localhost:port=27017:user=xyz:passwd=xxxx:db=name>')

def main():
    LogaggMasterCommand().start()

if __name__ == '__main__':
    main()
