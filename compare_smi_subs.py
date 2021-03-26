#!/usr/bin/python3
import sys
import subprocess

filename = sys.argv[0]
hostname = sys.argv[1]
port = sys.argv[2]
username = sys.argv[3]
password = sys.argv[4]

command1 = "show subscriber count { all }"
command2 = "show ipam dp"
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
        self.com2()
        self.compare()
    def com1(self):
        ssh_command = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.host, '-p',self.port, command1],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)
        output = ssh_command.stdout.read().decode("utf-8") 
        for i in output.splitlines():
            if 'sessionCount' in i:
                self.sescount = int(i.split(":")[1])
                break
            else:
                self.sescount = 0
    def com2(self):
        ssh_command = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.host, '-p',self.port, command2],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)
        output = ssh_command.stdout.read().decode("utf-8")
        self.ipv6prefix = 0
        for i in output.splitlines():
            if ':' in i:
                upf = i.split(" ")[0]
                self.upf_list.append(upf.split(":")[0])
                ssh_command = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.host, '-p',self.port, command2,upf],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT)
                output1 = ssh_command.stdout.read().decode("utf-8")
                for j in output1.splitlines():
                    if 'Ipv6Prefix' in j:
                        pre = j.split(" ")[6]
                        self.upf_ipam_value.append(pre)
                        self.ipv6prefix = self.ipv6prefix + int(pre) 
    def compare(self):
        if self.sescount == self.ipv6prefix:
            print("Number of sessions are same %s" % self.sescount)
            print("Pass")
        else:
            print("Fail")
            #print("Different")
        print("show subscriber count { all } : %s" % self.sescount)
        print("show ipam dp : %s" % self.ipv6prefix)
        print("====\nDetails of show ipam dp")
        for i in range(len(self.upf_list)):
            print("UPF IP: %s | Used IPv6 Prefix: %s" % (self.upf_list[i], self.upf_ipam_value[i]))
        




if __name__=="__main__":
    if len(sys.argv) != 5:
        usage()
    ssh(hostname, port, username, password)
