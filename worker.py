import common
import zmq
import socket
import argparse
import json
import datetime


class JobWorker():

    def __init__(self,args):

        worker_ip = common.get_client_ip(args.server)
        self.context = zmq.Context()

        #setup message queue for work messages, bind to random port to allow for multiple workers per host
        socket_pull=self.context.socket(zmq.PULL)
        worker_port = socket_pull.bind_to_random_port('tcp://{0}'.format(worker_ip))
        worker_ip_port='{0}:{1}'.format(worker_ip,worker_port)

        socket_sub = self.context.socket(zmq.SUB)
        socket_sub.connect("tcp://{}:{}".format(args.server, common.server_pub_port))
        socket_sub.setsockopt(zmq.SUBSCRIBE,'worker')

        socket_req = self.context.socket(zmq.REQ)
        socket_req.connect("tcp://{}:{}".format(args.server, common.server_port))

        self.socket_logger = self.context.socket(zmq.PUSH)
        self.should_continue = True
        self.socket_push = socket_req
        self.socket_sub = socket_sub
        self.socket_pull = socket_pull
        self.worker_ip_port = worker_ip_port
        self.logger_ip_port = None

        self.all_workers = set()
        self.config = {}
        self.threads = []
        self.logger_ip_port = None
        self.logger = None
        self.job = None

        #'waiting' -> 'running' -> 'finish' -> waiting
        self.state = 'waiting'

        print ("worker starting with ip:{}".format(self.worker_ip_port))
        self.notify_server()

    def send_log(self,msg,type):
        if self.logger_ip_port:
            msg['worker'] = self.worker_ip_port
            msg['type'] = type
            self.socket_logger.send_json(msg)
        else:
            print('trying to send log with no logger: "{}":{}'.format(type,msg))



    def connect_logger(self,logger_ip_port):
        #todo close existing logger

        if logger_ip_port != self.logger_ip_port:
            self.logger_ip_port = logger_ip_port
            self.socket_logger.connect('tcp://{}'.format(logger_ip_port))


    def notify_server(self,cmd='new_worker'):
        msg = {'cmd':cmd,'worker': self.worker_ip_port}
        self.socket_push.send_json(msg)
        self.socket_push.recv()


    def process_server_control(self,topic,msg):

        msg = json.loads(msg)
        print "Recieved control Topic: %s  cmd: %s" % (topic, str(msg))

        if msg['cmd'] == 'report':
            self.notify_server()

        elif msg['cmd'] == 'init':
            self.connect_logger(msg['logger'])
            self.job = common.job_workers[msg['job']]
            self.job.init(msg,self)
            self.notify_server(cmd='worker_ready')
            self.state = 'running'
            print("state == running")

        elif msg['cmd'] == 'finish':
            self.state = 'finish'

        else:
            print('uknown server control message: "{}":{}'.format(topic,msg))


    def process_server_work(self,msg):
        self.job.process_server_work(msg)

    def main_loop(self):

        #setup polling device
        poller = zmq.Poller()
        poller.register(self.socket_sub,zmq.POLLIN)
        poller.register(self.socket_pull,zmq.POLLIN)


        start = datetime.datetime.now()
        last_work = start
        last_heartbeat = start
        while self.should_continue:

            socks = dict(poller.poll(100))

            #Server Control Messages
            if self.socket_sub in socks and socks[self.socket_sub] == zmq.POLLIN:
                message = self.socket_sub.recv()
                topic = message.split()[0]
                msg = message[len(topic)+1:]
                self.process_server_control(topic,msg)

            now = datetime.datetime.now()

            #Work messages, could be from server or other worker
            if self.socket_pull in socks and socks[self.socket_pull] == zmq.POLLIN:
                msg = self.socket_pull.recv_json()
                self.process_server_work(msg)
                print("#")
                last_work = now


            if self.state == 'waiting' and (now-start) > datetime.timedelta(seconds=10):
                start = now
                #annouce ourselves to the server
                self.notify_server()
                print("annoucing to server")

            elif self.state == 'finish' and (now-last_work) > datetime.timedelta(seconds=5):
                #If we have received the finish cmd from server and it's been 5 seconds since last work item
                #we are finished as well.
                if self.job.is_finished():
                    self.state = 'waiting'
                    print("Finished! going back to waiting")
                    self.send_log({},'finished')

            if (self.state == 'running' or self.state == 'finish') and (now-last_heartbeat) > datetime.timedelta(seconds=10):
                #start sending heartbeats so the logger knows we are alive
                last_heartbeat=now
                self.send_log({},'heartbeat')
                print("sending heartbeat")





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s",'--server',help='Server IP',required=True)
    args = parser.parse_args()

    #init and start JobWorker
    JobWorker(args).main_loop()


if __name__ == "__main__":
    main()