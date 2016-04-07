import zmq
import common
import argparse
import json
import time


class JobManager():

    def __init__(self,args):
        #Setup Control ports for publishing config messages to workers and receiving
        self.args = args
        self.job = common.job_servers[args.job]
        self.job.init(args)

        self.context = zmq.Context()
        self.socket_pub = self.context.socket(zmq.PUB)
        self.socket_pub.bind('tcp://*:%s' % common.server_pub_port)

        self.socket_rep = self.context.socket(zmq.REP)
        self.socket_rep.bind('tcp://*:%s' % common.server_port)
        self.poller = zmq.Poller()
        self.poller.register(self.socket_rep,zmq.POLLIN)

        self.socket_logger = self.context.socket(zmq.PUSH)
        self.socket_work = self.context.socket(zmq.PUSH)

        self.workers = set()
        self.ready_workers = set()
        self.logger_ip_port = None

        #Loop until all clients connect
        self.state_ready = False


        self.nonce = str(time.time())

    def send_control(self,msg,topic='worker'):
        self.socket_pub.send('{} {}'.format(topic,json.dumps(msg)))


    def connect_logger(self,logger_ip_port):
        if logger_ip_port != self.logger_ip_port:
            self.logger_ip_port = logger_ip_port
            self.socket_logger.connect('tcp://{}'.format(logger_ip_port))

    def send_log(self,msg,type):
        if self.logger_ip_port:
            msg['worker'] = 'server'
            msg['type'] = type
            self.socket_logger.send_json(msg)
        else:
            print('trying to send log with no logger: "{}":{}'.format(type,msg))



    def connect_workers(self):
        for worker in self.ready_workers:
            self.socket_work.connect('tcp://{}'.format(worker))


    def process_msg(self):

        #sleep for 5 seconds wait for message
        socks = dict(self.poller.poll(5000))

        if self.socket_rep in socks and socks[self.socket_rep] == zmq.POLLIN:
            msg = self.socket_rep.recv_json()

            print("-> msg:{}".format(msg))

            if msg['cmd'] == 'new_worker':
                self.workers.add(msg['worker'])
                self.socket_rep.send_json("ok")

            elif msg['cmd'] == 'new_logger':
                #pass along message to all the workers
                self.connect_logger(msg['logger'])
                self.socket_rep.send_json("ok")

            elif msg['cmd'] == 'worker_ready':
                self.ready_workers.add(msg['worker'])
                self.socket_rep.send_json("ok")

            elif msg['cmd'] == 'logger_finished':
                self.socket_rep.send_json("ok")
                exit()

            else:
                self.socket_rep.send_json('Error: Unkown cmd')


    def send_work(self,msg):
        self.socket_work.send_json(msg)

    def stage_job(self):
        while self.logger_ip_port is None or len(self.ready_workers) < self.args.workers :
            print("Logger: {}".format(self.logger_ip_port))
            print("Worker Count: {}".format(len(self.workers)))
            self.process_msg()

            #if we have met the minimum node count start sending new inits on each new node
            if self.logger_ip_port and len(self.workers) >= self.args.workers:
                print('Sending job init cmd')
                self.send_job_init()

        #We Are ready
        self.connect_workers()

    def send_job_init(self):
        #Send job initalization information, block until all workers are ready, connect to workers
        self.send_log({'msg':"Starting job: {0}".format(self.job.get_name())},type='info')
        job_config = self.job.init(self.args)
        self.send_control({'cmd':'init',
                           'nonce':self.nonce,
                           'job':self.job.get_name(),
                           'threads':self.args.threads,
                           'workers':list(self.workers),
                           'logger':self.logger_ip_port,
                           'job_config':job_config})


    def run_job(self):
        for item in self.job.get_work():
            self.send_work(item)

    def wait_job(self):
        self.job.wait_job()

    def finish_job(self):
        finish_config = self.job.finish_job()
        self.send_control({'cmd':'finish',
                           'job':self.job.get_name(),
                           'finish_config':finish_config})

    def wait_finish(self):
        while True:
            self.process_msg()



def main():

    #Parse Command Line
    parser = argparse.ArgumentParser()
    parser.add_argument('-j','--job',choices=common.job_servers.keys(),required=True)
    parser.add_argument('-w','--workers',type=int,required=True,help='Number of worker processes')
    parser.add_argument('-t','--threads',type=int,default=16,help='Number of threads per worker')

    #Create job instances, this adds additional command line params
    for job in common.job_servers :
        common.job_servers[job].add_arguments(parser)

    #loop through all job types and add thier own params
    args = parser.parse_args()
    job_manager = JobManager(args)


    #Wait for everyone to connect
    print('Waiting for workers and logger to connect and init')
    job_manager.stage_job()

    print('Sending work items')
    job_manager.run_job()

    print('waiting for work to complete')
    job_manager.wait_job()

    print('all work sent')
    job_manager.finish_job()


    job_manager.wait_finish()



if __name__ == "__main__":
    main()