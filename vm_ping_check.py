#!/usr/bin/python
##############################################################################################
############################### vm_ping_check.py #############################################
##############################################################################################
###    
### USAGE:
###     vm_ping_check.py -h                                   Print this message
###		vm_ping_check.py <host> <port> <username <password>	  Specify all four for correct functionality 


import paramiko
from paramiko import SSHClient
import sys, getopt,subprocess
import yaml, json
import ipaddress

def printhelp():
	print "usage:vm_ping_check.py <host> <port> <username> <password>"

def ping(ip, timeout = 0.01):
	cmd = "timeout " + str(timeout) + " ping " + str(ip) + " -c 1"
	p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
	output, error = p.communicate()
	if p.returncode != 0: 
		return False
	else:
		return True

def main():
	if len(sys.argv) == 1 or str(sys.argv[1]) == "-h" or str(sys.argv[1]) == "--help":
		printhelp()
		exit(0)
	elif len(sys.argv) != 5:
		print "ERROR: incorrect command line arguments"
		printhelp()
		exit(1)

	ssh = SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
	try:
		ssh.connect(sys.argv[1], port=int(sys.argv[2]), username=sys.argv[3], password=sys.argv[4], banner_timeout=5,allow_agent=False,look_for_keys=False)
	except Exception as e:
		print(e)
		print "ERROR: cannot connect - check arguments"
		exit(1)

	ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("kubectl get nodes | egrep '\-data-|-ims'")
	host_file_content = ssh_stdout.read()
	#print host_file_content
	# print vm_hosts
	vmtransport = ssh.get_transport()
	local_addr = (sys.argv[1], 22)
	hostlines = host_file_content.splitlines()
	hostlines = [line.strip() for line in hostlines
                 if not line.startswith('#') and line.strip() != '']

	for line in hostlines:
		hostname = line.split()[0]

		dest_addr = (hostname, 22)
		vmchannel = vmtransport.open_channel("direct-tcpip", dest_addr, local_addr)
		
		vmhost = paramiko.SSHClient()
		vmhost.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		#vmhost.load_host_keys('/home/osmanl/.ssh/known_hosts') #disabled#
		try:
			vmhost.connect(hostname,password=sys.argv[4], username=sys.argv[3],sock=vmchannel,banner_timeout=5,allow_agent=False,look_for_keys=False)
			print "Connected to " + hostname
		except Exception as e:
			print (e)
			exit()

		stdin, stdout, stderr = vmhost.exec_command("cat /etc/netplan/50-cloud-init.yaml")
		try:
			y = yaml.safe_load(stdout.read())
			#print "Loaded yaml file"
		except Exception as e:
			print (e)
			exit()

		colout_init_json = json.loads(json.dumps(y))
		failed = list()
		# print 
		for vlan_name in colout_init_json['network']['vlans']:
			vlan = colout_init_json['network']['vlans'][vlan_name]

			if "n40" in vlan_name:
				cmd = 'timeout 0.5 curl --verbose --http2-prior-knowledge -X POST -H "Content-Type:application/json" --data "@Mar_create_http2.json" http://10.191.26.65:1090/nchf-convergedcharging/v2/chargingdata'
				stdin, stdout, stderr = vmhost.exec_command(cmd)
				#p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
				#output, error = p.communicate()
				output = str(stdout.read())
				check = '{"cause":"INVALID_MSG_FORMAT","detail":"Payload not adhering to json schema","status":400}'
				if check in output: 
					print "10.191.26.65 success"
				else:
					print "10.191.26.65 fail"

				cmd = 'timeout 0.5 curl --verbose --http2-prior-knowledge -X POST -H "Content-Type:application/json" --data "@Mar_create_http2.json" http://10.191.153.129:1090/nchf-convergedcharging/v2/chargingdata'
				stdin, stdout, stderr = vmhost.exec_command(cmd)
				#p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
				#output, error = p.communicate()
				output = str(stdout.read())
				check = '{"cause":"INVALID_MSG_FORMAT","detail":"Payload not adhering to json schema","status":400}'
				if check in output: 
					print "10.191.153.129 success"
				else:
					print "10.191.153.129 fail"
				continue

			"""yml_routes = vlan.get('routes')
			if yml_routes:
				for route in yml_routes:
					to = str(unicode(route['to']))
					net = ipaddress.ip_network(unicode(to), strict=False)
					for ip in net:
						if ping(ip):
							print str(ip) + " success"
						else:
							print str(ip) + " fail"
							failed.append(ip)
					#print repr(failed)
					for x in failed:
						if ping(x):
							print str(x) + " success"
							failed.remove(x)
						else:
							print str(x) + " fail"""

		vmhost.close()
	ssh.close()
	# End

	#ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("ifconfig")
	# ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("show running-config | nomore")
	# print ssh_stdout.read()
	
if __name__ == '__main__':
   main()






