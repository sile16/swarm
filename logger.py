import zmq
import sys
import common
import time
import argparse
import json
import datetime
import curses

stdscr = None

class JobLogger():

    def __init__(self,args):
        #Get client IP
        logger_ip_port = "{}:{}".format(common.get_client_ip(args.server), common.logging_port)


        #Setup logging Port
        self.context = zmq.Context()
        socket_logs = self.context.socket(zmq.PULL)
        socket_logs.bind('tcp://*:{}'.format(common.logging_port))

        #Setup to receive published messages from server
        socket_sub = self.context.socket(zmq.SUB)
        socket_sub.connect("tcp://{}:{}".format(args.server, common.server_pub_port))
        socket_sub.setsockopt(zmq.SUBSCRIBE,'worker')
        socket_sub.setsockopt(zmq.SUBSCRIBE,'logger')

        #Setup to push messages to Server
        socket_req = self.context.socket(zmq.REQ)
        socket_req.connect("tcp://{}:{}".format(args.server, common.server_port))

        self.socket_sub = socket_sub
        self.socket_logs = socket_logs
        self.socket_req = socket_req
        self.logger_ip_port = logger_ip_port
        self.state = 'waiting'

        print ("logger starting with ip:{}".format(logger_ip_port))
        self.notify_server()

        self.job = None

        self.workers = None
        self.heartbeats = {}
        self.finished_workers = set()


    def notify_server(self):
        msg = {'cmd':'new_logger','logger': self.logger_ip_port}
        self.socket_req.send_json(msg)
        self.socket_req.recv()



    def process_server_control(self,topic,msg):
        print("Received control Topic: {}  cmd: {}".format(topic, msg))
        if topic == 'worker':
            if msg['cmd'] == 'init':
                self.job = common.job_loggers[msg['job']]
                self.heartbeats = {w:datetime.datetime.now() for w in msg['workers']}
                self.workers = msg['workers']
                self.finished_workers = set()
                self.job.init(msg)
                self.state = 'running'

            elif msg['cmd'] == 'finish':
                self.state = 'finish'


    def process_log(self,msg):
        if msg['type'] == 'heartbeat':
            self.heartbeats[msg['worker']] = datetime.datetime.now()

        elif msg['type'] == 'finished':
            self.finished_workers.add(msg['worker'])

        if self.job:
            self.job.process_log(msg)
        else:
            print("log: {}".format(msg))

    def finish(self):
        self.job.finish()



    def display(self):
        #stdscr.addstr("testing123")
        #stdscr.addstr(self.finished_workers)
        #stdscr.refresh()
        pass





    def main_loop(self):
        #setup polling device
        poller = zmq.Poller()
        poller.register(self.socket_sub,zmq.POLLIN)
        poller.register(self.socket_logs,zmq.POLLIN)


        print('logger starting loop ')
        should_continue = True

        start = datetime.datetime.now()
        last_log = datetime.datetime.now()
        last_display = datetime.datetime.now()
        while should_continue:
            socks = dict(poller.poll(100))

            if self.socket_sub in socks and socks[self.socket_sub] == zmq.POLLIN:
                message = self.socket_sub.recv()
                topic = message.split()[0]
                msg = json.loads(message[len(topic)+1:])
                self.process_server_control(topic,msg)

            if self.socket_logs in socks and socks[self.socket_logs] == zmq.POLLIN:
                msg = self.socket_logs.recv_json()
                self.process_log(msg)
                last_log = datetime.datetime.now()


            now = datetime.datetime.now()
            if self.state == 'waiting' and (now-start) > datetime.timedelta(seconds=10):
                self.notify_server()
                start = datetime.datetime.now()

            elif self.state == 'finish' and (now-last_log) > datetime.timedelta(seconds=5):
                last_log = now
                for worker in self.workers:
                    if worker not in self.finished_workers:
                        if (now - self.heartbeats[worker]) > datetime.timedelta(seconds=10):
                            #worker stopped sending heartbeats.  Mark as dead
                            self.finished_workers.add(worker)
                            print("worker dead: {}".format(worker))

                #Once all workers are either finished or timedout we are done:
                if len(self.finished_workers) == len(self.workers):
                    #we are done
                    self.state = 'waiting'
                    print("Finished! going to waiting state")
                    self.job.finish()
                    msg = {'cmd':'logger_finished','logger': self.logger_ip_port}
                    self.socket_req.send_json(msg)
                    self.socket_req.recv()


            if (now-last_display) > datetime.timedelta(seconds=5):
                self.display()




def main():
    #stdscr = curses_screen
    #stdscr.clear()
    parser = argparse.ArgumentParser()
    parser.add_argument("-s",'--server',help='Server IP',required=True)
    args = parser.parse_args()

    JobLogger(args).main_loop()



if __name__ == "__main__":
    #curses.wrapper(main)
    main()