#!/usr/bin/python2
import sys
import subprocess
import yaml
output = []

filename = sys.argv[0]
hostname = sys.argv[1]
port = sys.argv[2]
username = sys.argv[3]
password = sys.argv[4]

command1 = """show alerts active"""
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
        dict1 = {}
        ssh_command = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.host, '-p',self.port, command1],
                stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = ssh_command.stdout.read().decode("utf-8")
        Final = False
        print(output)
        
             
           
       
        
            
                
            
    
           
                
            
        
                
        
              
                
        
    
       

                    
                
       
       
          
           
               
    

if __name__=="__main__":
    if len(sys.argv) != 5:
        usage()
    ssh(hostname, port, username, password)
