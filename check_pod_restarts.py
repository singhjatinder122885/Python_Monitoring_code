#!/usr/bin/python2
import sys
import subprocess
import yaml
output_1 = []

filename = sys.argv[0]
hostname = sys.argv[1]
port = sys.argv[2]
username = sys.argv[3]
password = sys.argv[4]
ymlfile = '/root/python_scripts/crash_ymls/'+ sys.argv[1] + '_crash_count.yml'
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
        dict1 = {}
        ssh_command = subprocess.Popen(['sshpass','-p',self.password,'ssh', '-oStrictHostKeyChecking=no', self.user+'@'+self.host, '-p',self.port, command1],
                stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = ssh_command.stdout.read().decode("utf-8")
        Final = False
        
        for i in output.splitlines():
            i = ' '.join(i.split())
            #print(i)
            if i.strip().startswith("cluster"):
                pod_name = str(i.split(" ")[3])
                name_space = str(i.split(" ")[2])
                a = name_space + "_%" + pod_name
            if i.strip().startswith("restart"):
                b = int(i.split(" ")[1])
                dict1[a] = b
            
        try:
            with open(ymlfile)as file1:
                data = yaml.safe_load(file1)
           
        except:
            pass
       
        with open(ymlfile, 'w') as outfile1:
            yaml.dump(dict1, outfile1)
        #Now we have two dictionaries to compare dict1 and data


        data_keys = set(data.keys())
        dict1_keys = set(dict1.keys())


        missing_pods = data_keys.difference(dict1_keys)
        new_pods = dict1_keys.difference(data_keys)
        same_pods = data_keys.intersection(dict1_keys)
        if data == dict1:
            print("Pass")
        else:
            print("Fail")
            if len(list(missing_pods)) > 0:
                   print("Following Pods are missing compared to Last run")
                   print(list(missing_pods))
            if len(list(new_pods)) > 0:
                   print("Following are new PODs")
                   print(list(new_pods))
                   for check in list(new_pods):
                       if dict1[check] > 0:
                           print("{} got rebooted {} time since its redeployment").format(str(check),dict1[str(check)])
            for key in list(same_pods):
               if str(data[key]) != str(dict1[key]):
                  print('Pod that restarted are {} ({} vs. {})'.format(key, str(data[key]), str(dict1[key])))
            

        '''
        # data = previous value
        # dict1 = current value
        
        # PODxxx --> PODyyy
        #1. if keys exists in previus value, but not exists in current value: (POD is missing, can be becaused POD got redployed) - Print FAILED, Following PODs are missing : PODxxx
        #2. if keys not exists in previus value, but  exists in current value: (New POD, ) Print FAILED, Following PODs are new POD: PODyyyy
        #3. if keys is exists in both previus and current, then compare the key value : your existing logic
    
        missing_pod = []
        new_pod = []
	pod_restart = []
        # first condition - missing POD
		
        for old_pod in data.keys():
            # this will define missing POD
            if str(old_pod) not in dict1.keys():
                missing_pod.append(str(old_pod))
        
        # second condition - new POD
        for current_pod in dict1.keys():
            if str(current_pod) not in data.keys():
                new_pod.append(str(current_pod))
        #print(data)
        #print(dict1)
        try:
            for current_pod in dict1.keys():
                if dict1[current_pod] != data[current_pod]:
                    pod_restart.append(str(current_pod))        
        except:
            pass
        

        #output:

        if dict1 == data:
            print("Pass")        
        else:
            print("Failed")
            if(len(missing_pod) > 0):
                print("Following PODs are missing: ")
                print(missing_pod)
            if(len(new_pod) > 0):
                print("Following PODs are new:")
                print(new_pod)
            if(len(pod_restart) > 0):
                print("Following PODs are restarted:")
                print(pod_restart)
        '''

       
                    
            
if __name__=="__main__":
    if len(sys.argv) != 5:
        usage()
    ssh(hostname, port, username, password)
