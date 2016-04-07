# swarm

Example Usage

Python 2.7

Spin up 1 logger:
python logger.py -s [server IP]

spin up [N] workers:
python worker.py -s [server IP]

spin UP server:
time python server.py -j dns -w [N] --dns-server 192.168.142.150 --dns-query isi150.lappy.lab  --dns-count 100


Example Output on the logger:

Finished! going to waiting state
{u'192.168.142.151': 39922,

 u'192.168.142.152': 39921,
 
 u'192.168.142.153': 39919,
 
 u'192.168.142.154': 39919,
 
 u'192.168.142.155': 39919,
 
 u'error_empty': 0,
 
 u'error_exception': 0,
 
 u'error_nxdomain': 0,
 
 u'error_otherdns': 0,
 
 u'error_timeout': 0}
 
 
 
 
