#!/usr/bin/python2
import sys
import subprocess
import socket

filename = sys.argv[0]
region = sys.argv[1]
port = sys.argv[2]
username = "Msalame5"
password = "Zebra@123"

static_inventory = {'NE-Beltsville':{\
    'BE551SP1': '10.240.164.69',\
    'WBE551SP2': '10.240.164.70',\
    'WBE551LF1': '10.240.164.71',\
    'WBE551LF2': '10.240.164.72',\
    'WB551LF3': '10.240.164.73',\
    'WB551LF4': '10.240.164.74',\
    'WB551LF5': '10.240.164.75',\
    'WB551LF6': '10.240.164.76'},\
    'NE-Philadelphia': {\
    'PH551SP1': '10.240.171.133',\
    'PH551SP2': '10.240.171.134',\
    'PH551LF1': '10.240.171.135',\
    'PH551LF2': '10.240.171.136',\
    'PH551LF3': '10.240.171.137',\
    'PH551LF4': '10.240.171.138',\
    'PH551LF5': '10.240.171.139',\
    'PH551LF6': '10.240.171.140'},\
    'NW-WestSac': {\
    'WS551SP1': '10.254.87.132',\
    'WS551SP2': '10.254.87.133',\
    'WS551LF1': '10.254.87.134',\
    'WS551LF2': '10.254.87.135',\
    'WS551LF3': '10.254.87.136',\
    'WS551LF4': '10.254.87.137'},\
    'NW-Polaris': {\
    'PO551SP1': '10.254.200.68',\
    'PO551SP2': '10.254.200.69',\
    'PO551LF1': '10.254.200.70',\
    'PO551LF2': '10.254.200.71',\
    'PO551LF3': '10.254.200.72',\
    'PO551LF4': '10.254.200.73'},
    'South-West-Titan': {\
    'TI551SP1': '10.252.204.68',\
    'TI551SP2': '10.252.204.69',\
    'TI551LF1': '10.252.204.70',\
    'TI551LF2': '10.252.204.71',\
    'TI551LF3': '10.252.204.72',\
    'TI551LF4': '10.252.204.73'},
    'South-West-Houston': {\
    'HN551SP1': '10.240.140.132',\
    'HN551SP2': '10.240.140.133',\
    'HN551LF1': '10.240.140.134',\
    'HN551LF2': '10.240.140.135',\
    'HN551LF3': '10.240.140.136',\
    'HN551LF4': '10.240.140.137'},
    'Central-Aurora': {\
    'AU551SP1': '10.240.193.196',\
    'AU551SP2': '10.240.193.197',\
    'AU551LF1': '10.240.193.198',\
    'AU551LF2': '10.240.193.199',\
    'AU551LF3': '10.240.193.200',\
    'AU551LF4': '10.240.193.201'},
    'Central-Elgin': {\
    'EL551SP1': '10.240.253.132',\
    'EL551SP2': '10.240.253.133',\
    'EL551LF1': '10.240.253.134',\
    'EL551LF2': '10.240.253.135',\
    'EL551LF3': '10.240.253.136',\
    'EL551LF4': '10.240.253.137'},
    'West-RiverSide': {\
    'RV551SP1': '10.240.135.196',\
    'RV551SP2': '10.240.135.197',\
    'RV551LF1': '10.240.135.198',\
    'RV551LF2': '10.240.135.199',\
    'RV551LF3': '10.240.135.200',\
    'RV551LF4': '10.240.135.201'},
    'West-LasVegas': {\
    'LV551SP1': '10.240.146.68',\
    'LV551SP2': '10.240.146.69',\
    'LV551LF1': '10.240.146.70',\
    'LV551LF2': '10.240.146.71',\
    'LV551LF3': '10.240.146.72',\
    'LV551LF4': '10.240.146.73'},
    'SE-Orlando': {\
    'OR551SP1': '10.240.154.69',\
    'OR551SP2': '10.240.154.70',\
    'OR551LF1': '10.240.154.71',\
    'OR551LF2': '10.240.154.72',\
    'OR551LF3': '10.240.154.73',\
    'OR551LF4': '10.240.154.74',\
    'OR551LF5': '10.240.154.75',\
    'OR551LF6': '10.240.154.76'},
    'SE-Charlotte': {\
    'CR551SP1': '10.240.243.69',\
    'CR551SP2': '10.240.243.70',\
    'CR551LF1': '10.240.243.71',\
    'CR551LF2': '10.240.243.72',\
    'CR551LF3': '10.240.243.73',\
    'CR551LF4': '10.240.243.74',\
    'CR551LF5': '10.240.243.75',\
    'CR551LF6': '10.240.243.76'}}


command1 = " show bfd neighbors vrf all"
command2 = " show logging | inc %BFD-5-SESSION_STATE_DOWN"
ssh_cmd = 'ssh %s@%s %s -p %s % (username, hostname, port)'


def usage():
    print("usage: %s  <host> <port> <username> <password>" % filename)
    print("required apps: sshpass")

class ssh():
    def __init__(self, region, port, username, password):

        self.region = region
        self.port = port
        self.user = username
        self.password = password
        self.askpass = False 
        self.com1()
    def com1(self):
        for i in static_inventory[region]:
            '''
            OurAddr         NeighAddr       LD/RD                 RH/RS           Holdown(mult)     State       Int                   Vrf                              Type    
            192.168.114.1   192.168.114.0   1090519048/1090519041 Up              4851(3)           Up          Eth1/61               default                          SH      
            192.168.114.3   192.168.114.2   1090519049/1090519042 Up              4550(3)           Up          Eth1/62               default                          SH      
            192.168.224.1   192.168.224.0   1090519050/1090519041 Up              5771(3)           Up          Eth1/63               default                          SH      
            192.168.224.3   192.168.224.2   1090519051/1090519048 Up              5215(3)           Up          Eth1/64               default                          SH   
            '''                        
            self.ip = static_inventory[region][i]
            ssh_command = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.ip, '-p',self.port, command1],
                stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            #ssh_command_2 = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.ip, '-p',self.port, command2],
              #  stdout_2=subprocess.PIPE,stderr=subprocess.STDOUT)
            #output2 =  ssh_command_2.stdout.read().decode("utf-8")
            output = ssh_command.stdout.read().decode("utf-8")
            #print(output)
            #logging_output=subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.ip, '-p',self.port, command2],
                #stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            #logging_output1 = logging_output.stdout.read().decode("utf-8")
            #print(logging_output1)
            
            
            node_bfd_not_ok = []
            for output_data in output.splitlines():
                output_data = ' '.join(output_data.split()).split(" ")
                OurAddr_ip = output_data[0]
                try:
                     socket.inet_aton(OurAddr_ip)
                     neighbor_ip = output_data[1]
                     bfd_status = output_data[5]
                     if bfd_status != "Up":
                         node_bfd_not_ok.append(neighbor_ip)
                except socket.error:
                    pass
            if len(node_bfd_not_ok) == 0:
                print("Node %s is healthy" %(i))
                
            else:
                print("Node %s is not healthy, following BFD neighbor are down:" %(i))
                print(node_bfd_not_ok)
    
            
            

        

if __name__=="__main__":
    if len(sys.argv) != 5:
        usage()
        exit()
    
    if region not in static_inventory.keys():
        print("Region not supported")
        print("supported region: ") 
        for i in static_inventory.keys():
            print(i)
            #print(output2)
        exit()

    ssh(region, port, username, password)

