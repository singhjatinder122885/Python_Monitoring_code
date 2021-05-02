#!/usr/bin/python3
# version 1.3


import getopt, sys
import os
import re

fullCmdArguments = sys.argv


argumentList = fullCmdArguments[1:]
index = []
indextemp = []
variables = {}
var = {}
config = {}
config2 = {}
index = {}
hashlist = []

schemas = ['apn','apn-expansion','apn-qci-duration','card','context','dcca-group','diameter','diameter-acct','diameter-auth','dpca','ecs','egtpc','gtpc','gtpp','gtpu','imsa','ippool','lac','link-aggr','lns','p2p','pgw','pgw-egtpc-s2a','pgw-egtpc-s2b','pgw-egtpc-s5s8','port','ppp-per-pcf','radius','radius-group','rlf','rlf-detailed','rulebase','saege','schema','sgw','vlan-npu','vrf','ppp','hss','map','mon-di-net','sccp','sgs','sgtp','ss7link','ss7rd','tai']

for schema in schemas:
    variables[schema] = []
    var[schema] = []

if("-file"  in argumentList) and ("-config" in argumentList) and ("-node" in argumentList) and ("-pushgateway" in argumentList):
    bulkstatdata = argumentList[1+argumentList.index("-file")]
    bulkstatconfig = argumentList[1+argumentList.index("-config")]
    node = argumentList[1+argumentList.index("-node")]
    pushgateway = argumentList[1+argumentList.index("-pushgateway")]
else:
    print("Usage: python "+fullCmdArguments[0]+" -config [bulkstat config file] -file [bulkstat data file] -node [node name] -pushgateway [pushgatewayaddress:port]")
    print("supported schema : ")
    print(" ".join(list(map(str, schemas))))
    sys.exit()


for line in open(bulkstatconfig).readlines():
    for schema in schemas:
        schemaspace = schema + " "
        if line.lstrip().startswith(schemaspace):
            schemaformat = line.strip().split(" format ")[1]
            schemametrics = schemaformat.strip().split(",")
            for x in schemametrics:
                if("%" not in x):
                    variables[schema].append(x)

for schema in schemas:
    frequency = {}
    for item in variables[schema]:
        if (item in frequency):
            frequency[item] += 1
        else:
            frequency[item] = 1
    for key, value in frequency.items():
        if(value == 1 and re.findall('[0-9]', key)):
            var[schema].append(key)
    for line in open(bulkstatconfig).readlines():
        for a in var[schema]:
            if(" " + a + " " in line):
                config[a] = line.strip().split(" format ")[1].strip().split(",")
                config2[a] = list(map(lambda each:each.strip("%"), config[a]))
                if(schema == "ecs"):
                    index[a] = "node"
                elif(schema == "sccp"):
                    index[a] = "node"
                elif(schema == "rulebase"):
                    index[a] = "ecs-rbase-name"
                elif(schema == "dcca-group"):
                    index[a] = "cc-group"
                elif(schema == "p2p"):
                    if("%p2p-protocol%" in config[a]):
                        index[a]= "p2p-protocol"
                    elif("%p2p-duration-name%" in config[a]):
                        index[a]= "p2p-duration-name"
                elif(schema == "diameter"):
                    if("%endpoint-name%" in config[a]):
                        index[a] = "endpoint-name"
                    else:
                        index[a] = "vpnname"
                elif(schema == "apn-qci-duration"):
                    index[a] = "apn-name"
                elif(schema == "system"):
                    index[a] = "node"
                elif(schema == "map"):
                    index[a] = "servname"
                elif(schema == "sgs"):
                    index[a] = "servname"
                elif(schema == "schema"):
                    index[a] = "schema"                    
                elif(schema == "tai"):
                    index[a] = "tai-mcc-mnc-tac"
                elif(schema == "sgsn"):
                    index[a] = "mcc-mnc-lac-rac"
                elif(schema == "ss7rd"):
                    index[a] = "ss7rd-number-asp"
                elif(schema == "sgtp"):
                    index[a] = "sgtpindex"                                     
                else:
                    schemaid = "%" + schema + "%"
                    if(schemaid in config[a]):
                        index[a] = schema
                    elif("%vpn-name%" in config[a]):
                        index[a] = "vpn-name"
                    else:
                        index[a] = "vpnname"


f= open("tempfile.txt","w+")

for linedata in open(bulkstatdata).readlines():
    schemadata = linedata.strip().split(",")
    for schema in schemas:
        for item in var[schema]:
            for b in schemadata:
                if(b.startswith(item) and b.endswith(item)):
                    indexid = "%" + index[item] + "%"                   
                    if(index[item] == "node"):
                        label = " "
                    elif(index[item] == "schema"):
                        label = " "
                    elif(index[item] == "tai-mcc-mnc-tac"):
                        label = "{mcc=\"" + schemadata[config[item].index("%tai-mcc%")]+"\",mnc=\"" + schemadata[config[item].index("%tai-mnc%")] + "\",tac=\"" + schemadata[config[item].index("%tai-tac%")]+"\"} "
                    elif(index[item] == "mcc-mnc-lac-rac"):
                        label = "{mcc=\"" + schemadata[config[item].index("%mcc%")]+"\",mnc=\"" + schemadata[config[item].index("%mnc%")] + "\",lac=\"" + schemadata[config[item].index("%lac%")] + "\",rac=\"" + schemadata[config[item].index("%rac%")]+"\"} "
                    elif(index[item] == "ss7rd-number-asp"):
                        label = "{ss7rd=\"" + schemadata[config[item].index("%ss7rd-number%")]+"\",ss7asp=\"" + schemadata[config[item].index("%ss7rd-asp_instance%")]+"\"} "
                    elif(index[item] == "sgtpindex"):
                        label = ""
                        if(schemadata[config[item].index("%service-name%")] != ""):
                            label = "{service_name=\"" + schemadata[config[item].index("%service-name%")]+"\"} "
                        if(schemadata[config[item].index("%iups-service%")] != ""):
                            label = "{iups_service=\"" + schemadata[config[item].index("%iups-service%")]  +"\"} "
                        if(schemadata[config[item].index("%iups-service%")] != "" and schemadata[config[item].index("%service-name%")] != ""):
                            label = "{service_name=\"" + schemadata[config[item].index("%service-name%")]+"\"" + ",iups_service=\"" + schemadata[config[item].index("%iups-service%")]  +"\"} "
                        if(label == ""):
                            label = "{vpn_name=\"" + schemadata[config[item].index("%vpn-name%")]+"\"} "
                    else:
                        label = "{"+index[item].replace("-","_") + "=\"" + schemadata[config[item].index(indexid)] +"\"} "
                   
                    for data in schemadata:
                        time = schemadata[config[item].index("%epochtime%")]
                        if(data.isdigit() and data != "0" and config2[item][schemadata.index(data)] != "epochtime" and config2[item][schemadata.index(data)] != "localtime" and config2[item][schemadata.index(data)] != "localdate" and config2[item][schemadata.index(data)] != "uptime" and config2[item][schemadata.index(data)] != "vpnid" and config2[item][schemadata.index(data)] != "vpnname" and  config2[item][schemadata.index(data)] != "lac" and config2[item][schemadata.index(data)] != "rac" and config2[item][schemadata.index(data)] != "mnc" and config2[item][schemadata.index(data)] != "mcc" and config2[item][schemadata.index(data)] != "tai-mcc" and config2[item][schemadata.index(data)] != "tai-mnc" and config2[item][schemadata.index(data)] != "tai-tac" and config2[item][schemadata.index(data)] != "vpn-id" and config2[item][schemadata.index(data)] != "ss7rd-number" and config2[item][schemadata.index(data)] != "ss7rd-asp_instance" and config2[item][schemadata.index(data)] != "endtime"  and config2[item][schemadata.index(data)] != "enddate" and config2[item][schemadata.index(data)] != "localendtime"):
                            #print(config2[item][schemadata.index(data)])
                            metric = config2[item][schemadata.index(data)].lower().replace("-","_") + " "
                            # convertng metric into label
                            if(schema=="schema"):
                                if(config2[item][schemadata.index(data)].startswith("disc-reason-")):
                                    metric = "disc_reason "
                                    label = "{disc_reason=\"" + config2[item][schemadata.index(data)].lower().replace("-","_") + "\"} "
                                elif(config2[item][schemadata.index(data)].startswith("sess-bearerdur-")):
                                    metric = "session_bearer_duration "
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{duration=\"" + labs[2] + "\",qci=\"" + labs[3]+"\"} "
                                elif(config2[item][schemadata.index(data)].startswith("sess-setuptime")):
                                    metric = "session_setuptime "
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{duration=\"" + labs[2]+"\"} "      
                                elif(config2[item][schemadata.index(data)].startswith("sess-calldur")):
                                    metric = "session_callduration "
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{duration=\"" + labs[2]+"\"} "  
                                elif(config2[item][schemadata.index(data)].startswith("sess-rxpkt")):
                                    metric = "session_rxpacket "
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{packets=\"" + labs[2]+"\"} "    
                                elif(config2[item][schemadata.index(data)].startswith("sess-txpkt")):
                                    metric = "session_txpacket "
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{packets=\"" + labs[2]+"\"} "                                                                         
                                #elif(config2[item][schemadata.index(data)].startswith("cc-")):
                                #    print(config2[item][schemadata.index(data)])
                                #    metric = "credit_control "
                                #    labs = config2[item][schemadata.index(data)].strip().split("cc-")
                                #    label = "{cctype=\"" + labs[1].replace("-","_")+"\"} "     
                            if(schema=="apn"):             
                                if(config2[item][schemadata.index(data)].startswith("qci")):         
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    if(len(labs)==2):
                                        metric = labs[1]
                                        label = " {apn=\""+schemadata[config[item].index(indexid)]+"\",qci=\"" + labs[0].replace("-","_")+"\"} "
                                    elif(len(labs)==3):
                                        metric = labs[1]
                                        label = " {apn=\""+schemadata[config[item].index(indexid)]+"\",type=\""+labs[2] +"\",qci=\"" + labs[0].replace("-","_")+"\"} "
                                    elif(len(labs)==4):
                                        metric = labs[1]
                                        label = " {apn=\""+schemadata[config[item].index(indexid)]+"\",type=\""+labs[2] + "\",other=\""+labs[3]+"\",qci=\"" + labs[0].replace("-","_")+"\"} "       
                            elif(schema=="card"):             
                                if(config2[item][schemadata.index(data)].startswith("npuutil")):   
                                    metric = "npuutil "   
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{card=\""+schemadata[config[item].index(indexid)]+"\",duration=\"" + labs[1]+"\"} "  
                            elif(schema=="p2p"):  
                                metric = "p2p"     
                                #print(config2[item][schemadata.index(data)]) 
                                if("p2p-duration-value" in config2[item][schemadata.index(data)]):
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{type=\""+labs[1]+"\",unit=\"s\",p2p_name=\""+schemadata[config[item].index("%p2p-duration-name%")]+"\"} "
                                else:
                                    labs = config2[item][schemadata.index(data)].strip().split("-")
                                    label = "{type=\""+labs[1]+"\",unit=\"" + labs[2]+"\",p2p_name=\""+schemadata[config[item].index(indexid)]+"\"} "
                                                              
                            hashtemp = []
                            hashtemp.append(schema)
                            hashtemp.append(config2[item][schemadata.index(data)])
                            hashtemp.append(label)
                            a = hash(str(hashtemp))

                            if(a not in hashlist):
                               
                                f.write(schema.lower().replace("-","_") + "_" + metric +label +data+"\n")

                                hashlist.append(a)
f.close()
cmd = 'cat tempfile.txt |  curl --data-binary @- http://' + pushgateway +'/metrics/job/bulkstat/node/' +node
os.system(cmd)
