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
        parser.add_argument("--dns-count",help='number of queries',type=int,default=100 )
        parser.add_argument("--dns-rate-limit",help='# of requests per second max',type=int,default=600 )
        parser.add_argument("--dns-burst-amount", help='number of burst requests',type=int,default=2000)
        parser.add_argument("--dns-burst-count", help='number of burst requests',type=int,default=2)

    def get_name(self):
        return "dns"

    def init(self,args):
        '''
        Initialize for new job run
        :return: data to be passed to clients needed to initialize
        '''
        self.args = args
        if args.dns_server is None or args.dns_query is None:
            print('missing required DNS arguments')
            print(args)
            self.parser.print_help()
            quit()

        #return arguments to be passed to workers
        return {'server': args.dns_server,'query': args.dns_query}

    def get_work(self):
        for x in range(1,self.args.dns_count):
            msg = {'cmd':'query','count':100}
            yield msg

    def wait_job(self):
        pass

    def finish_job(self):
        pass


class DNSJobWorkerThread(threading.Thread):

    def __init__(self,queue,stats,job_config,lock):
        threading.Thread.__init__(self)
        self.queue = queue
        self.stats = stats
        self.job_config = job_config
        self.lock=lock
        self.should_continue = True

    def run(self):

        my_resolver = dns.resolver.Resolver()
        my_resolver.nameservers = [self.job_config['server']]

        while self.should_continue:
            msg = self.queue.get()

            try:
                answer = my_resolver.query(self.job_config['query'])
                count=0

                with self.lock:
                    for rdata in answer:
                        count+=1
                        if rdata.address not in self.stats:
                            self.stats[rdata.address]=1
                        else:
                            self.stats[rdata.address]+=1
                    if count == 0:
                        self.stats['error_empty'] +=1


            except dns.resolver.NXDOMAIN:
                with self.lock:
                    self.stats['error_nxdomain']+=1
            except dns.resolver.Timeout:
                with self.lock:
                    self.stats['error_exception']+=1
            except dns.exception.DNSException:
                with self.lock:
                    self.stats['error_otherdns']+=1
                traceback.print_exc()
            except Exception, e:
                with self.lock:
                    self.stats['error_exception']+=1
                traceback.print_exc()

            self.queue.task_done()

    def stop(self):
        self.should_continue = False



class DNSJobWorker:

    def __init__(self):
        self.args = None
        self.queue = None
        self.stats = None
        self.threads = []

    def init(self,msg,worker):
        #this function can be called multiple times with different params

        self.stats = {}
        self.stats['error_empty'] = 0
        self.stats['error_exception'] = 0
        self.stats['error_nxdomain'] = 0
        self.stats['error_timeout'] = 0
        self.stats['error_otherdns'] = 0

        self.thread_count = msg['threads']
        self.job_config = msg['job_config']
        self.worker = worker
        self.queue = Queue.Queue()
        lock = threading.Lock()

        for t in self.threads:
            t.stop()

        self.threads = []

        for x in range(1,self.thread_count):
            t = DNSJobWorkerThread(self.queue,self.stats,self.job_config,lock)
            t.setDaemon(True)
            t.start()
            self.threads.append(t)


    def process_server_work(self,msg):
        #ideally this is a non-blocking call so that we can send heartbeats to the logger

        for x in range(msg['count']):
            self.queue.put(msg)

    def is_finished(self):
        if self.queue.empty():
            self.worker.send_log({'stats':self.stats},'stats')
            return True

        return False


class DNSJobLogger():

    def __init__(self):
        self.stats = {}

    def init(self,msg):
        self.stats = {}
        print("clearing stats")

    def process_log(self,log):

        if 'stats' in log:
            for key in log['stats']:
                if key in self.stats:
                    self.stats[key] += log['stats'][key]
                else:
                    self.stats[key] = log['stats'][key]

    def finish(self):
        pprint(self.stats)





