#!/usr/bin/python3
import sys
import subprocess

filename = sys.argv[0]
hostname = sys.argv[1]
port = sys.argv[2]
username = sys.argv[3]
password = sys.argv[4]

command1 = "show cluster pods"
ssh_cmd = 'ssh %s@%s %s -p %s % (username, hostname, port)'


def usage():
    print("usage: %s  <host> <port> <username> <password>" % filename)
    print("required apps: sshpass")

class ssh():
    def __init__(self, hostname, port, username, password):
        self.upf_list = []
        self.upf_ipam_value = []
        self.host = hostname
        self.port = port
        self.user = username
        self.password = password
        self.askpass = False 
        self.com1()
    def com1(self):
        ssh_command = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.host, '-p',self.port, command1],
                stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = ssh_command.stdout.read().decode("utf-8")
        ready  = True
        status = True
        failure_node = []
        '''
        cluster pods pcf-wsp network-query-ks7rf
        ready      1/1
        status     Running
        node       wspcf-smi-cluster-policy-protocol1
        ip         10.192.1.3
        restarts   0
        start-time 2020-06-24T09:32:03Z
        '''
        for i in output.splitlines():
            
            i = ' '.join(i.split())
            if i.strip().startswith("cluster"):
                ready = True
                status = True
            if i.strip().startswith("node"):
                node = i.split(" ")[1]
            if i.strip().startswith("ready"):
                status = i.split(" ")[1]              
                for j in status:
                    if status.split("/")[0] != status.split("/")[1]:
                        ready = False
            if i.strip().startswith("status"):
                stat = i.split(" ")[1]
                if stat != "Running":
                    status = False
            if i.strip().startswith("start-time"):
                if not ready and not status:
                    failure_node.append(node)
        
        if len(failure_node) == 0:
            print("Pass")
            print(output)
        else:
            print("Fail")
            print("Failure Node : ")
            print(failure_node)
            print("Over all output is {}").format(output)

if __name__=="__main__":
    if len(sys.argv) != 5:
        usage()
    ssh(hostname, port, username, password)
