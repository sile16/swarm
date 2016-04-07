import threading
import Queue
import dns.resolver
from pprint import pprint
import traceback
import dns.exception


class DNSJobServer():

    def __init__(self):
        self.args = None
        self.clients = set()
        self.parser = None

    def add_arguments(self,parser):
        self.parser = parser
        parser.add_argument("--dns-server",help='dns server client requests go to, Required' )
        parser.add_argument("--dns-query",help='dns name to query, Required')
        parser.add_argument("--dns-duration",help='length of test in seconds',default=10 )
        parser.add_argument("--dns-rate-limit",help='# of requests per second max',default=600 )
        parser.add_argument("--dns-burst-amount", help='number of burst requests',default=2000)
        parser.add_argument("--dns-burst-count", help='number of burst requests',default=2)

    def get_name(self):
        return "dns"

    def init(self,args):
        '''
        Initialize for new job run
        :return: data to be passed to clients needed to initialize
        '''
        self.args = args
        print('initializing DNS Job Server')
        if args.dns_server is None or args.dns_query is None:
            print('missing required DNS arguments')
            print(args)
            self.parser.print_help()

            quit()

        return {'server': args.dns_server,'query': args.dns_query}

    def get_work(self):
        for x in range(1,100):
            msg = {'cmd':'query','count':5}
            yield msg

    def wait_job(self):
        pass

    def finish_job(self):
        pass


class DNSJobWorkerThread(threading.Thread):

    def __init(self,queue):
        threading.Thread.__init__(self)
        #self.worker_ip = worker_ip


    def run(self):

        while True:
            pass


class DNSJobWorker:

    def __init__(self):
        self.args = None
        self.queue = None
        self.stat_current = None

    def init(self,msg,worker):
        self.threads = msg['threads']
        self.job_config = msg['job_config']
        self.worker = worker


        self.queue = Queue.Queue()


    def process_server_work(self,msg):

        return

        #ideally this is a non-blocking call so that we can send heartbeats to the logger

        my_resolver = dns.resolver.Resolver()
        my_resolver.nameservers = [self.job_config['server']]

        stat_current = {}

        stat_current['error_empty'] = 0
        stat_current['error_exception'] = 0
        stat_current['error_nxdomain'] = 0
        stat_current['error_timeout'] = 0
        stat_current['error_otherdns'] = 0

        for x in range(msg['count']):
            try:
                answer = my_resolver.query(self.job_config['query'])

                count=0
                for rdata in answer:
                    count+=1
                    if rdata.address not in stat_current:
                        stat_current[rdata.address]=1
                    else:
                        stat_current[rdata.address]+=1
                if count == 0:
                    stat_current['error_empty'] +=1

            except dns.resolver.NXDOMAIN:
                stat_current['error_nxdomain']+=1
            except dns.resolver.Timeout:
                stat_current['error_exception']+=1
            except dns.exception.DNSException:
                stat_current['error_otherdns']+=1
                traceback.print_exc()
            except Exception, e:
                stat_current['error_exception']+=1
                traceback.print_exc()

        self.worker.send_log(stat_current,'stats')


    def is_finished(self):
        return True


class DNSJobLogger():

    def __init__(self):
        self.stat_current = {}

    def init(self,msg):
        self.stat_current = {}

    def process_log(self,log):
        for key in log:
            if key in self.stat_current:
                self.stat_current[key] += log[key]
            else:
                self.stat_current[key] = log[key]

    def finish(self):
        pprint(self.stat_current)





