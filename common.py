import socket
import dnsjob


server_port = 22300
server_pub_port = 22301
worker_port = 22400
logging_port = 22500

job_servers = {'dns':dnsjob.DNSJobServer()}
job_workers = {'dns':dnsjob.DNSJobWorker()}
job_loggers = {'dns':dnsjob.DNSJobLogger()}


def get_client_ip(server):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((server,server_pub_port))
    worker_ip = s.getsockname()[0]
    s.close()
    return worker_ip


