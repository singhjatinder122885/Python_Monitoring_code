#!/usr/bin/python
##############################################################################################
############################### backupconfig.py ##############################################
##############################################################################################
### SUMMARY:
###    This script ssh's to a host and prints the config to the screen
###    
### USAGE:
###     backupconfig -h                                   Print this message
###		backupconfig <host> <port> <username <password>	  Specify all four for correct functionality 
### AUTHOR: JSingh


import paramiko
from paramiko import SSHClient
import sys, getopt
import yaml, json
import ipaddress

def printhelp():
	print "usage:vm_route_check.py  <host> <port> <username> <password>"

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

	#print repr(sys.argv[1])
	#print repr(int(sys.argv[2]))
	#print repr(sys.argv[3])
	#print repr(sys.argv[4])

    
	try:
		ssh.connect(sys.argv[1], port=int(sys.argv[2]), username=sys.argv[3], password=sys.argv[4], banner_timeout=5,allow_agent=False,look_for_keys=False)
	except Exception as e:
		print(e)
		print "ERROR: cannot connect - check arguments"
		exit(1)

	ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cat /etc/hosts | grep \"data-\|ims-\"")
	host_file_content = ssh_stdout.read() 
	# print vm_hosts
	vmtransport = ssh.get_transport()
	local_addr = (sys.argv[1], 22)
	hostlines = host_file_content.splitlines()
	hostlines = [line.strip() for line in hostlines
                 if not line.startswith('#') and line.strip() != '']

	for line in hostlines:
		hostip = line.split()[0]
		hostname = line.split()[1]
		# hostnames = line.split('#')[0].split()[1:]
		print hostname
		dest_addr = (hostip, 22)
		vmchannel = vmtransport.open_channel("direct-tcpip", dest_addr, local_addr)
		#
		vmhost = paramiko.SSHClient()
		vmhost.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		#vmhost.load_host_keys('/home/osmanl/.ssh/known_hosts') #disabled#
		vmhost.connect(hostip,password=sys.argv[4], username=sys.argv[3],sock=vmchannel,banner_timeout=5,allow_agent=False,look_for_keys=False)
		#
		stdin, stdout, stderr = vmhost.exec_command("route -n | awk '{print $1\" \"$2\" \"$3\" \"$8}'")
		#
		vm_routes = stdout.read()
		# print vm_routes
		stdin, stdout, stderr = vmhost.exec_command("cat /etc/netplan/50-cloud-init.yaml ")
		# routes = 
		y=yaml.load(stdout.read())
		colout_init_json = json.loads(json.dumps(y))
		# print 
		for vlan_name in colout_init_json['network']['vlans']:
			# print vlan_name
			vlan = colout_init_json['network']['vlans'][vlan_name]
			yml_routes = vlan.get('routes')
			if yml_routes:
				for route in yml_routes:
					to = str(unicode(route['to']))
					via = str(unicode(route['via']))
					net = ipaddress.ip_network(unicode(to), strict=False)
					to_ip = net.network_address
					to_mask = net.netmask
					yaml_route =  str(to_ip) + " " + via + " " + str(to_mask) + " " + vlan_name
					if yaml_route not in vm_routes:
						# print vlan_name + " to: " + to + " = PASS"
					# else:
						print vlan_name + " to: " + to +  " via " + via + " Missing!!"
			# else:
				# print "no routes in vlan: "+vlan_name
		#
		vmhost.close()
	ssh.close()
	# End

	#ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("ifconfig")
	# ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("show running-config | nomore")
	# print ssh_stdout.read()
	
if __name__ == '__main__':
   main()






