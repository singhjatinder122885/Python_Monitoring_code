root@cxtool:~/python_scripts # cat nf_ping_check_new.py 
#!/usr/bin/python3
##############################################################################################
############################### vm_route_check_new.py ########################################
##############################################################################################
###    
### USAGE:
###     vm_route_check_new.py -h                                      Print this message
###             vm_route_check_new.py <host> <port> <username <password>          Specify all four for correct functionality 



import paramiko
from paramiko import SSHClient
import sys
import time
import json
import concurrent.futures

def printhelp():
        print("usage: " + sys.argv[0] + " <host> <port> <username> <password>....")

def ping(vmhost, address, nf_name):
        stdin, stdout, stderr = ssh.exec_command("time ping -c 4 " + ip)
        ping_output = stdout.read().decode('ascii').strip("\n")
    #print(ping_output)
        if not " 100% packet loss" in ping_output:
                print("ping to "+nf_name + " ip "+ address + " pass")
        else:
                print("ping to "+nf_name + " ip "+ address + " failed!!")


def main():
        if len(sys.argv) == 1 or str(sys.argv[1]) == "-h" or str(sys.argv[1]) == "--help":
                printhelp()
                exit(0)
        elif len(sys.argv) != 7:
                print("ERROR: incorrect command line arguments")
                printhelp()
                exit(1)

        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
                ssh.connect(sys.argv[1], port=int(sys.argv[2]), username=sys.argv[3], password=sys.argv[4], banner_timeout=5,allow_agent=False,look_for_keys=False)
        except Exception as e:
                print(e)
                print("ERROR: cannot connect - check arguments")
                exit(1)
        cli = "kubectl get nodes | grep '"+sys.argv[6]+"' | egrep 'proto'"
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cli)
        host_file_content = ssh_stdout.read()
        print(cli)
        print(host_file_content)
        vmtransport = ssh.get_transport()
        local_addr = (sys.argv[1], 22)
        hostlines = host_file_content.splitlines()
        region = (sys.argv[5]).replace(" ", "")

        for line in hostlines:
                hostname = line.split()[0]

                dest_addr = (hostname, 22)
                vmchannel = vmtransport.open_channel("direct-tcpip", dest_addr, local_addr)

                vmhost = paramiko.SSHClient()
                vmhost.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                try:
                        vmhost.connect(hostname,password=sys.argv[4], username=sys.argv[3],sock=vmchannel,banner_timeout=5,allow_agent=False,look_for_keys=False)
                        print("\n\n******* Connected to " + hostname + " *******")
                        with open('nf_region_ips_ping_check.json') as nf_ip_file:
                                data = json.load(nf_ip_file)
                                for region_data in data['nf_sheet_json']:
                                        print("\n>>>>> pinging " + region + " 1")
                                        if region in region_data['region']:
                                                print("\n>>>>> pinging " + region + " 2")
                                                for nfs in region_data['nfs']:
                                                        print("\n>>>>> pinging " + region + " 3")
                                                        nf_name = nfs['nf_name']
                                                        print("\n>>>>> pinging " + region + " " + nf_name)

                                                        pool = concurrent.futures.ThreadPoolExecutor(10)

                                                        for ip_data in nfs['ip_data']:
                                                                address = ip_data['ip']
                                                                #info = ip_data['type']

                                                                pool.submit(ping, vmhost, address, nf_name)
                                                                #pool.shutdown(wait=True)

                except Exception as e:
                        print(e)
                        exit()
                print("Done!!")
                vmhost.close()

        ssh.close()

if __name__ == '__main__':
   main()
