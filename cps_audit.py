#!/usr/bin/env python3.4
#!/usr/local/bin/python3

import os, errno, sys, time, datetime, select, re, logging, argparse, ast, string, threading
import json, cx_Oracle, http.client, requests, zlib, base64, gzip, binascii

# Mongo DB Support
from pymongo import MongoClient
from bson import BSON
from bson import json_util

from cps_core_dev import core_check_running, core_create_dir, core_mongo_diag, core_strip_seq_char, core_yes_or_no, core_convert_date
import xml.dom.minidom as minidom
from pysimplesoap.client import SoapClient, SimpleXMLElement

# SPR MySQL Handler
import pymysql

# Supress warnings by ignoring the SSL certificates for the CPS API
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings() 

# Static Variables
map_pod = {     '1': 'pod1prvg',
                '6': 'pod6prvg' }

mapper = {
   'cps': {
      'lab': {
           'cluman': { 'sjc': '10.5.36.109', 'phx': '10.5.36.209' },
              'api': { 'sjc': '10.5.36.200', 'phx': '10.5.36.205' },
         'username': 'root',
         'password': '321Jasper'
      },
      'prod': {
           'cluman': { 'sjc': '10.1.49.9',   'phx': '10.3.49.9'   },
              'api': { 'sjc': '10.1.49.10',  'phx': '10.3.49.10'  },
         'username': 'root',
         'password': '..J45per'
      }
   },
   'ora': {
      'lab': {
             'host': { 'mvs_a': '10.5.51.19', 'mvs_b': '10.5.51.20', 'mvs': '10.5.51.21' },
         'username': 'root',
         'password': '4Jasper0nly'
      },
      'prod': {
             'host': { 'sjc_a': '10.16.51.19', 'sjc_b': '10.16.51.20', 'sjc': '10.16.51.21',
                       'phx_a': '10.18.51.19', 'phx_b': '10.18.51.20', 'phx': '10.18.51.21' },
         'username': 'root',
         'password': '..J45per'
      }
   }
}

map_apirouter = { 'set05': '01234', 'set08': '56789' }

oracle_adhoc_user = 'loguser'
oracle_adhoc_pass = 'jasper5log'

# Collect provided IDs into a clean list
sub_list = dict()
sub_list['imsi'] = []
sub_list['msisdn'] = []
sub_list['subid'] = []

# Custom Class Objects
class subscriber:
   def __init__(self):
      self.date          = ""
      self.subid         = ""
      self.imsi          = ""
      self.msisdn        = ""
      self.policy        = ""
      self.alter_speed   = False
      self.reduce_qos    = False
      self.source        = ""
      self.service       = ""

cps_cfg_file_base = '/data/datapcrf/cps_tools/cps_mongo_cfg_cache.'
cps_service_code_map_file = '/data/datapcrf/cps_tools/svc_code_mapping.csv'
service_code_map = dict()

#############################################################
# ARGPARSE: Base Parameters
usage_text = "This tool is used to perform actions related to query/audit/sync of subscriber state across all SPR Data Sources"
parser = argparse.ArgumentParser(description=usage_text, formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=30))
subparsers = parser.add_subparsers(dest='action')

# ARGPARSE: Cache CPS Mongo data locally
parser_cps_cfg_cache = subparsers.add_parser('cps_cfg_cache',
                                              help='Refresh the local Mongo Configuration Cache.')
parser_cps_cfg_cache.add_argument(   '--env',
                                              help='Target CPS Mongo DB from which to cache configuration data.',
                                              choices=mapper['cps'].keys(),
                                              required=True,
                                              type=str)

# ARGPARSE: Export Options - Oracle SPR
parser_export = subparsers.add_parser('export',
                                              help='Export SPR database to CSV file.')
parser_export.add_argument(       '--system',
                                              help='Target SPR System from which to export data (CPS or Oracle).',
                                              choices=['cps','oracle'],
                                              required=True,
                                              type=str)
parser_export.add_argument(          '--env',
                                              help='Target Environment from which to export (Lab or Production).',
                                              choices=['lab','prod'],
                                              required=True,
                                              type=str)
parser_export.add_argument(      '--csvfile',
                                              help='File to write exported CSV data.',
                                              required=True,
                                              type=str)
parser_export.add_argument(       '--target',
                                              help='Target Oracle SPR instance from which to export data (Oracle only).',
                                              choices=['mvs_a','mvs_b','sjc_a','sjc_b','phx_a','phx_b'],
                                              type=str)
parser_export.add_argument(        '--after',
                                              help='Limit to entries modified after UTC date (YYYY_MM_DD_HH). Affects Oracle only.',
                                              type=str)
parser_export.add_argument(       '--before',
                                              help='Limit to entries modified before UTC date (YYYY_MM_DD_HH). Affects Oracle only.',
                                              type=str)


# ARGPARSE: cpsdb: Check one or more subscribers for sanity across the CPS Mongo DB schema with option to delete
parser_cpsdb = subparsers.add_parser('cpsdb',
                                              help='Check a subscriber directly on CPS Mongo DB')
parser_cpsdb.add_argument(           '--env',
                                              help='Target Environment (Lab or Production).',
                                              choices=['lab','prod'],
                                              required=True,
                                              type=str)
parser_cpsdb.add_argument(          '--imsi',
                                              help="IMSI or file with list of IMSIs. Multiple allowed.",
                                              action='append',
                                              type=str)
parser_cpsdb.add_argument(        '--msisdn',
                                              help="MSISDN or file with list of MSISDNs. Multiple allowed.",
                                              action='append',
                                              type=str)
parser_cpsdb.add_argument(         '--subid',
                                              help="SubID or file with list of SubIDs. Multiple allowed.",
                                              action='append',
                                              type=str)
parser_cpsdb.add_argument(       '--balance',
                                              help="Include Balance Lookup.",
                                              action='store_true')
parser_cpsdb.add_argument(       '--session',
                                              help="Include Session Lookup.",
                                              action='store_true')
parser_cpsdb.add_argument(        '--status',
                                              help="Display only a OK/ERROR output without any verbosity.",
                                              action='store_true')
parser_cpsdb.add_argument(         '--count',
                                              help="Show a sum of counts from all CPS DB tables.",
                                              action='store_true')
parser_cpsdb.add_argument(        '--delete',
                                              help="Delete from 'spr' (+SK), 'balance', or 'session' on DB. Use only if API fails. Multiple allowed.",
                                              action='append',
                                              type=str)
parser_cpsdb.add_argument(         '--force',
                                              help=argparse.SUPPRESS,
                                              action='store_true')

# ARGPARSE: Sync Options with Control Center and/or Oracle SPR
parser_sync = subparsers.add_parser(  'sync', 
                                              help='Check and correct sync state for individual subscribers.')
parser_sync.add_argument(            '--env',
                                              help='Target Environment (Lab or Production).',
                                              choices=['lab','prod'],
                                              required=True,
                                              type=str)
parser_sync.add_argument(           '--imsi', 
                                              help="Individual IMSI, or file with list of IMSIs to audit. Multiple allowed.",
                                              required=True,
                                              action='append',
                                              type=str)
parser_sync.add_argument(         '--msisdn', 
                                              help=argparse.SUPPRESS)
parser_sync.add_argument(          '--subid', 
                                              help=argparse.SUPPRESS)
parser_sync.add_argument(            '--pod', 
                                              help="Use CC DB as source based on specified PODs (default [6,1] first match). Multiple allowed.",
                                              choices=['1', '6'],
                                              action='append',
                                              type=str)
parser_sync.add_argument(         '--oracle', 
                                              help="Use Oracle SPR API as source from named.",
                                              choices=['mvs_a','mvs_b','sjc_a','sjc_b','phx_a','phx_b'],
                                              type=str)
parser_sync.add_argument(            '--cps', 
                                              help="CPS Site to query.",
                                              choices=['sjc','phx'],
                                              required=True,
                                              type=str)
parser_sync.add_argument(      '--recommend',
                                              help="Recommend changes to re-sync CPS with Oracle or CC data.",
                                              action='store_true')
parser_sync.add_argument(            '--fix',
                                              help="Execute the recommended changes immediately.",
                                              action='store_true')
parser_sync.add_argument(        '--verbose',
                                              help="Output the comparative data for each user (default is only errors).",
                                              action='store_true')

# ARGPARSE: Compare previously taken export dumps and identify mis-matches by IMSI
parser_compare= subparsers.add_parser('compare',
                                              help='Compare previously taken export dumps and identify mis-matches by IMSI')
parser_compare.add_argument(  '--spr_export',
                                              help="CSV File from Oracle SPR export previously taken with this tool.",
                                              required=True,
                                              type=str)
parser_compare.add_argument(  '--cps_export',
                                              help="CSV File from CPS export previously taken with this tool.",
                                              required=True,
                                              type=str)

# ARGPARSE: Global Options
parser.add_argument(               '--debug', 
                                              help="Run in verbose DEBUG mode.",
                                              default=0,
                                              action='store_true')

args = parser.parse_args()

#############################################################
# Unexpected error requires program exit
def quitNow(function, message):
   print('\nERROR: {}(): {}\n'.format(function, message))
   core_check_running(args.debug,os.path.basename(__file__))
   sys.exit(1)

#############################################################
# Verify arguments provided, and determine Subscriber ID if another key was provided
def validate_parameters():

   # Display help if there is no action specified
   if not args.action:
      parser.print_help(sys.stderr)

   # Check parameters related to export
   if args.action == 'export':
      if args.system == 'oracle':
         if not args.target:
            quitNow('validate_parameters', 'Must specify a target system when exporting Oracle data.')
         elif args.target.startswith('mvs') and args.env == 'prod':
            quitNow('validate_parameters', 'Specified target {} not in specified {} environment.'.format(args.target, args.env))
         elif not args.target.startswith('mvs') and args.env == 'lab':
            quitNow('validate_parameters', 'Specified target {} not in specified {} environment.'.format(args.target, args.env))

      # Check and convert to epoch the before date
      if args.before:
         if re.match('^[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]_[0-9][0-9]$', args.before):
            #args.before = core_convert_date(args.debug, 'epoch', 'YYYY_MM_DD_HH', args.before)
            pass
         else:
            quitNow('validate_parameters', 'Specified UTC before data is not well formatted: {} (YYYY_MM_DD_HH).'.format(args.before))

      # Check and convert to epoch the after date
      if args.after:
         if re.match('^[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]_[0-9][0-9]$', args.after):
            #args.after = core_convert_date(args.debug, 'epoch', 'YYYY_MM_DD_HH', args.after)
            pass
         else:
            quitNow('validate_parameters', 'Specified UTC after data is not well formatted: {} (YYYY_MM_DD_HH).'.format(args.after))


   # Check parameters related to subscriber
   if args.action == 'cpsdb' and not args.count:
      if not args.imsi and not args.msisdn and not args.subid:
         quitNow('validate_parameters', 'Must specify one or more identities (--imsi, --msisdn, --subid).')

   # If any specified IMSI is a valid file, scrap the IMSIs from that file
   if args.action == 'sync' or args.action == 'cpsdb':

      if args.imsi:   sub_list['imsi']   = args.imsi
      if args.msisdn: sub_list['msisdn'] = args.msisdn
      if args.subid:  sub_list['subid']  = args.subid
   
      for id_type in sub_list:
         for this_id in sub_list[id_type].copy():
            if os.path.isfile(this_id):
               sub_list[id_type].remove(this_id)
               if args.debug: print('DEBUG: validate_parameters(): Matched file for --{} specification: {}'.format(id_type.upper(), this_id))
               with open(this_id, 'rt') as id_file:
                  for id_value in id_file:
                     id_value = id_value.lstrip().rstrip()
                     if args.debug: print('DEBUG: validate_parameters(): Appending {} {} from file {}.'.format(id_type.upper(), id_value, this_id))
                     sub_list[id_type].append(id_value)
            else:
               if args.debug: print('DEBUG: validate_parameters(): Appending {} {} from CLI.'.format(id_type.upper(), this_id))
               sub_list[id_type].append(this_id)

   # Clean up any duplicates from the sub_list
   sub_list['imsi']   = list(set(sub_list['imsi']))
   sub_list['msisdn'] = list(set(sub_list['msisdn']))
   sub_list['subid']  = list(set(sub_list['subid']))

   # Replace the pod list if one is epcified on the CLI
   if args.action == 'sync':
      if not args.pod and not args.oracle:
         quitNow('validate_parameters', 'Must specify one or both of --pod and/or --oracle.')


#############################################################
# Query Subscriber on Control Center ADHOC Database (fast-sync replicate of production)
def queryCcDb(cc_db_cursor, pod_list, this_imsi):
   
   result = subscriber()

   # Go through the PODs for a first-match return
   for this_pod in pod_list:
      
      query_sql = ("select a.imsi, a.msisdn, c.name, d.name, d.value "
                   "from                      imsis@{db} a "
                   "left join          simauxfields@{db} b on (a.simid = b.simid) "
                   "left join              policies@{db} c on (c.policyid = b.policyid) "
                   "left join SimAdditionalPolicies@{db} d on (d.simid = a.simid AND d.policyid = b.policyid) "
                   "where a.imsi='{imsi}'"
                   .format(db=map_pod[this_pod], imsi=this_imsi) )

      if args.debug: print("DEBUG: queryCcDb(): query_sql={}".format(query_sql))

      # Run the query against the open DB cursor
      cc_db_cursor.execute(query_sql)

      for row in cc_db_cursor.fetchall():
         result.source = "POD" + this_pod
         result.imsi   = row[0]
         result.msisdn = row[1]
         result.policy = row[2]
         if row[3] and row[3].lower() == 'alter_speed':
            result.alter_speed = row[4].lower()
         if row[3] and row[3].lower() == 'reduce_qos':
            result.reduce_qos = True
         
      if result.source:
         return result

   # if no hits, set the pod to zero to signal no matches
   return result

#############################################################
# Query Oracle SPR API and return matches based on IMSI search
def querySprApi(spr_api_connection, this_imsi):

   if args.debug: print("DEBUG: querySprApi(): spr_api_connection={}, this_imsi={}".format(spr_api_connection, this_imsi))

   result = subscriber()
   result.source = 'SPR'

   # Added local for connection issue
   spr_api_connection = http.client.HTTPConnection(mapper['ora'][args.env]['host'][args.oracle], 8787)

   spr_api_headers = {"Content-type": "application/camiant-msr-v1+xml", "Accept": "*/*"}
   spr_api_connection.request("GET", "/rs/msr/sub/IMSI/" + this_imsi, '', spr_api_headers)

   try:
      api_response = spr_api_connection.getresponse()
   except:
      print("ERROR: Query for IMSI {} failed to Oracle SPR API.".format(this_imsi))
   else:
      if api_response.status == 200 or api_response.status == 201:
         result_xml = minidom.parseString(api_response.read().decode("utf-8"))
         xml_fields = result_xml.getElementsByTagName('field')
         if xml_fields:
            result.source = "SPR"
            for field in xml_fields:
               if field.getAttribute("name") == 'IMSI':
                  result.imsi = field.firstChild.nodeValue
               elif field.getAttribute("name") == 'MSISDN':
                  result.msisdn = field.firstChild.nodeValue
               elif field.getAttribute("name") == 'Entitlement':
                  if field.firstChild.nodeValue.startswith('reduce_qos'):
                     result.reduce_qos = True
                  elif field.firstChild.nodeValue.startswith('alter_speed'):
                     result.alter_speed = ''.join(field.firstChild.nodeValue.split("_")[-1:])
                  else:
                     result.policy = field.firstChild.nodeValue
      else:
         if args.debug: print("ERROR: Unexpected Result Code {} for IMSI {} on Oracle SPR API.".format(api_response.status, this_imsi))

   return result


#############################################################
# Query Oracle SPR API and return matches based on IMSI search
def queryCpsApi(cps_api_connection, this_imsi):

   # cps_api_connection = http.client.HTTPSConnection(mapper['cps'][args.env]['api'][args.cps], 443)
   result = subscriber()
   result.source = 'CPS'

   soap_label = 'https://{}:{}/ua/soap'.format(mapper['cps'][args.env]['api'][args.cps], '443')

   if args.debug: print('DEBUG: queryCpsApi(): soap_label={}'.format(soap_label))

   ### SimpleSoap no WSDL
   client = SoapClient(
      location = soap_label,
      action = soap_label,
      namespace = soap_label,
      soap_ns='soap',
      trace = args.debug,
      ns = False,
      exceptions=True)
   response = client.GetSubscriberRequest( networkId = this_imsi )

   if not response is None:
      try:
         if str(response.credential) != "":
            for cred in response.credential:
               if str(cred.type) == 'IMSI':
                  result.imsi = str(cred.networkId)
               if str(cred.type) == 'primary':
                  result.msisdn = str(cred.networkId)
      except:
         pass

      try:
         if str(response.subscriber.avp) != "":
            for avp in response.avp:
               if str(avp.code) == 'CCAR_POLICY':
                  result.policy = str(avp.value)
               if str(avp.code) == 'CCAR_THROTTLE':
                  result.alter_speed = str(avp.value)
               if str(avp.code) == 'CCAR_QCI':
                  result.reduce_qos = True
      except:
         pass

   return result


#############################################################
# Export Oracle DB to a CSV file
def exportOracle(env, csvfile, target):

   print("\nExporting Oracle SPR data from {} instance {} ({}) to file {}.".format(env, 
                                                                                   target, 
                                                                                   mapper['ora'][env]['host'][target],
                                                                                   csvfile))

   db = pymysql.connect(mapper['ora'][env]['host'][target], "root", "root", "bluedb" )
   cursor = db.cursor()
   start_time = int(time.time())
   write_counter = 0
   next_status = 10

   # Get the count for tracking
   cursor.execute('select count(*) from hsssprprofilerepositorydata')
   sql_count = re.sub(r'[^\d.]+', '', str(cursor.fetchone()))

   # Run the Export
   print("Exporting {} Database ({})...    ".format(target, sql_count),end='', flush=True)
   sql_dump = 'select ActiveSubsTimeStamp,PublicIdentity,ServiceData from hsssprprofilerepositorydata'

   # Add the date range is required
   clauses = []

   if args.after:
      clauses.append('ActiveSubsTimeStamp > "{}" '.format(datetime.datetime.strptime(args.after, '%Y_%m_%d_%H').strftime('%Y-%m-%d %H:00:00')))

   if args.before:
      clauses.append('ActiveSubsTimeStamp < "{}" '.format(datetime.datetime.strptime(args.before, '%Y_%m_%d_%H').strftime('%Y-%m-%d %H:00:00')))

   if clauses:
      sql_dump = sql_dump + " where " + ' and '.join(clauses)

   if args.debug: print('DEBUG: exportOracle(): Export Query: {}'.format(sql_dump))

   # Execute Export Query
   cursor.execute(sql_dump)

   with open(csvfile, 'w') as csv_file:
      for row in list(cursor.fetchall()):
         write_counter = write_counter + 1
         result = subscriber()
         result.source = target
         result.date = row[0]
         result.subid = row[1]
   
         service_data = zlib.decompress(row[2].encode('latin1'))
         xml_fields = minidom.parseString(service_data).getElementsByTagName('field')
   
         if xml_fields:
            for field in xml_fields:
               if field.getAttribute("name") == 'IMSI':
                  result.imsi = field.firstChild.nodeValue
               elif field.getAttribute("name") == 'MSISDN':
                  result.msisdn = field.firstChild.nodeValue
               elif field.getAttribute("name") == 'Entitlement' and field.firstChild:
                  if field.firstChild.nodeValue.startswith('reduce_qos'):
                     result.reduce_qos = True
                  elif field.firstChild.nodeValue.startswith('alter_speed'):
                     result.alter_speed = ''.join(field.firstChild.nodeValue.split("_")[-1:])
                  else:
                     result.policy = field.firstChild.nodeValue
   
         # Now print fetched result
         csv_file.write( "source={source},"
                          "subid={subid},"
                           "imsi={imsi},"
                         "msisdn={msisdn},"
                         "policy={policy},"
                    "alter_speed={alter_speed},"
                     "reduce_qos={reduce_qos},"
                           "date={date}\n".format( source=result.source,
                                                    subid=result.subid,
                                                     imsi=result.imsi,
                                                   msisdn=result.msisdn,
                                                   policy=result.policy,
                                              alter_speed=result.alter_speed,
                                               reduce_qos=result.reduce_qos,
                                                     date=result.date ))

         if int((write_counter / int(sql_count)) * 100) > next_status:
            print("\b\b\b{}%".format(next_status),end='', flush=True)
            next_status = next_status + 10

      print("\b\b\bDONE ({})".format(write_counter), flush=True)

   # disconnect from server
   db.close()

   print("\nCompleted Oracle SPR Export in {} seconds ({} TPS)\n".format(
      int(time.time() - start_time),
      int(write_counter / (time.time() - start_time)) ))


#############################################################
# Export CPS DB to a CSV file
def exportCps(env, csvfile, mongo_config):

   print("\nExporting CPS SPR data from {} to file {}.".format(env, csvfile))

   with open(csvfile, 'w') as csv_file:
      for db_set in mongo_config['SPR']:
         this_target = mongo_config['SPR'][db_set]

         mongo_client = MongoClient(this_target['ip_addr'], int(this_target['port']))
         mongo_db = mongo_client.spr
         start_time = int(time.time())
         write_counter = 0
         next_status = 10
   
         sql_count = mongo_db.subscriber.count()
   
         print("Exporting {} Database ({})...    ".format(db_set, sql_count),end='', flush=True)
         for row in mongo_db.subscriber.find():
            write_counter = write_counter + 1
            result = subscriber()
            result.source = db_set

            # Extract the Subscriber ID
            if '_id_key' in row:
               result.subid = row['_id_key']
      
            # Extract the MSISDN and IMSI
            if 'credentials_key' in row:
               for credential in row['credentials_key']:
                  if 'type_key' in credential and 'network_id_key' in credential:
                     if credential['type_key'] == 'primary':
                        result.msisdn = credential['network_id_key']
                     elif credential['type_key'] == 'IMSI':
                        result.imsi = credential['network_id_key']
      
            # Extract AVPs
            if 'avps_key' in row:
               for avp in row['avps_key']:
                  if 'code_key' in avp and 'value_key' in avp:
                     if avp['code_key'] == 'CCAR_POLICY':
                        result.policy = avp['value_key']
                     elif avp['code_key'] == 'CCAR_THROTTLE':
                        result.alter_speed = avp['value_key']
                     elif avp['code_key'] == 'CCAR_QCI':
                        result.reduce_qos = avp['value_key']

            # Extract the Service Code (future use)
            if 'services_key' in row:
               for service in row['services_key']:
                  if 'code_key' in service and 'enabled_key' in service and service['enabled_key'] == True:
                     result.service = service['code_key']
   
            # Now print fetched result
            csv_file.write("source={source},"
                           "subid={subid},"
                           "imsi={imsi},"
                           "msisdn={msisdn},"
                           "policy={policy},"
                           "alter_speed={alter_speed},"
                           "reduce_qos={reduce_qos},"
                           "service={service}\n".format( source=result.source,
                                                          subid=result.subid,
                                                           imsi=result.imsi,
                                                         msisdn=result.msisdn,
                                                         policy=result.policy,
                                                    alter_speed=result.alter_speed,
                                                     reduce_qos=result.reduce_qos,
                                                        service=result.service ))
   
            if int((write_counter / int(sql_count)) * 100) > next_status:
               print("\b\b\b{}%".format(next_status),end='', flush=True)
               next_status = next_status + 10
   
         print("\b\b\bDONE ({})".format(write_counter), flush=True)
   
      # disconnect from server
      mongo_client.close()

      print("\nCompleted CPS SPR Export in {} seconds ({} TPS)\n".format(
         int(time.time() - start_time),
         int(write_counter / (time.time() - start_time)) ))


#############################################################
## Validate all entries in CPS for a named subscriber
def cpsSchemaPerSub(id_type, this_id, mongo_db):

   # Itterate over all provided IDs
   found = dict()
   found['subscriber']         = []
   found['apirouter_sk_cache'] = []
   found['account']            = []
   found['session']            = []

   # Subscriber Profile
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'SPR':
         if args.debug: print('DEBUG: cpsSchemaPerSub(): Query SPR/{} for {} {}.'.format(db_handle['db_set'], id_type, this_id))
         found['subscriber'] = found['subscriber'] + cpsMongoQuery(db_handle, 'subscriber', id_type, this_id)

   # Adjust the handles to IMSI/MSISDN for SK Cache Lookup
   if len(found['subscriber']) > 0 and 'credentials_key' in found['subscriber'][0]['result'] and id_type == 'subid':
      for cred in found['subscriber'][0]['result']['credentials_key']:
         if cred['type_key'] == 'IMSI':
            id_type = 'imsi'
            this_id = cred['network_id_key']
         elif cred['type_key'] == 'primary':
            id_type = 'msisdn'
            this_id = cred['network_id_key']
            
   # Secondary Key Cache
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'SPR' and (id_type == 'msisdn' or id_type == 'imsi'):
         if args.debug: print('DEBUG: cpsSchemaPerSub(): Query SPR/{} for {} {}.'.format(db_handle['db_set'], id_type, this_id))
         found['apirouter_sk_cache'] = found['apirouter_sk_cache'] + cpsMongoQuery(db_handle, 'apirouter_sk_cache', id_type, this_id)

   # Replace the query ID if MSISDN or IMSI was provided, and balance or session is requested
   if len(found['subscriber']) > 0 and '_id_key' in found['subscriber'][0]['result'] and not id_type == 'subid':
      id_type = 'subid'
      this_id = found['subscriber'][0]['result']['_id_key']
   
   # Balance
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'BALANCE' and args.balance:
         if args.debug: print('DEBUG: cpsSchemaPerSub(): Query BALANCE/{} for {} {}.'.format(db_handle['db_set'], id_type, this_id))
         found['account'] = found['account'] + cpsMongoQuery(db_handle, 'account', id_type, this_id)
   
   # Session
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'SESSION' and args.session:
         if args.debug: print('DEBUG: cpsSchemaPerSub(): Query SESSION/{} for {} {}.'.format(db_handle['db_set'], id_type, this_id))
         found['session'] = found['session'] + cpsMongoQuery(db_handle, 'session', id_type, this_id)

   # Display the match results in debug
   if args.debug:
      print('MATCHING DB ENTRIES:\n--------------------')
      for match_type in found:
         for match in found[match_type]:
            print('MATCH on {}/{}/{}/{}:\n\n    {}\n'.format( match['db_handle']['db_type'],
                                                              match['db_handle']['db_set'],
                                                              match['db_handle']['db_shard'],
                                                              match_type,
                                                              match))

   # Return results
   return found

###############################################################
## Examine results from CPS Mongo query and report anomolies
def cpsValidateSubscriber(db_matches):

   # Setup error list to return
   errors = []

   if args.debug: print('DEBUG: cpsValidateSubscriber(): db_matches={}'.format(db_matches))
   
   # Check: There is a single unique SPR entry
   if len(db_matches['subscriber']) < 1:
      errors.append('VALIDATION FAIL: No matching entries found in CPS (SPR/subscriber).')
   elif len(db_matches['subscriber']) > 1:
      errors.append('VALIDATION FAIL: Multiple matching entries found in CPS (SPR/subscriber).')

   # Check: There are exactly one of each ID Type across all data collections
   imsi, msisdn, subid = set(), set(), set()

   for match in db_matches['subscriber']:
      if '_id_key' in match['result']:
         subid.add(match['result']['_id_key'])
      if 'credentials_key' in match['result']:
         for cred in match['result']['credentials_key']:
            if cred['type_key'] and cred['type_key'] == 'primary':
               msisdn.add(cred['network_id_key'])
            elif cred['type_key'] and cred['type_key'] == 'IMSI':
               imsi.add(cred['network_id_key'])

   for match in db_matches['apirouter_sk_cache']:
      if '_id' in match['result']:
         msisdn.add(match['result']['_id'])
      if 'tags' in match['result']:
         for tag in match['result']['tags']:
            if tag.startswith('ApiRouterSecondaryKey:networkId:'):
               cred = ''.join(tag.split(":")[-1:])
               if cred != match['result']['_id']:
                  imsi.add(cred)
               else:
                  msisdn.add(cred)

   if len(imsi)   != 1:
      errors.append('VALIDATION FAIL: Expected 1 IMSI; found {}: {}'.format(  len(imsi),   imsi  ))
   if len(msisdn) != 1:
      errors.append('VALIDATION FAIL: Expected 1 MSISDN; found {}: {}'.format(len(msisdn), msisdn))
   if len(subid)  != 1:
      errors.append('VALIDATION FAIL: Expected 1 SubID; found {}: {}'.format( len(subid),  subid ))

   # Check: There are two matching entries in SPR/SK Cache
   if len(db_matches['apirouter_sk_cache']) != 2:
      errors.append('VALIDATION FAIL: Expected 2 SK Cache entries; found {} (SPR/apirouter_sk_cache).'.format(len(set(db_matches['apirouter_sk_cache']))))

   # Check: The SPR record is in the expected set based on last digit ## map_apirouter = { 'set05': '01234', 'set08': '56789' }
   for match in db_matches['subscriber']:
      if 'credentials_key' in match['result']:
         for cred in match['result']['credentials_key']:
            if cred['type_key'] and cred['type_key'] == 'primary':
               if not list(cred['network_id_key'])[-1][-1] in list(map_apirouter[match['db_handle']['db_set']]):
                  errors.append('VALIDATION FAIL: SPR Profile in unexpected set: {} / {}'.format(
                    match['db_handle']['db_set'], map_apirouter[match['db_handle']['db_set']]))

   # Check: There is a balance for this subscriber if they are meant to have one
   # PENDING

   # Check: The CCAR_POLICY AVP is present if this is a Connected Car account
   # PENDING

   # Return Errors
   return errors


#############################################################
## Display the results and any found errors
def cpsShowSubscriber(db_matches, errors, status_flag):

   if status_flag:
      if len(errors) == 0:
         print('OK')
      else:
         print('ERROR')
      return
      
   # Print Matches: subscriber
   print('MATCHING DB ENTRIES:\n--------------------')
   for match_type in db_matches:
      for match in db_matches[match_type]:
         # Remove the padding and uncompress the data for a BALANCE match
         if match['db_handle']['db_type'] == 'BALANCE':
            if '_padding' in match['result']:
               match['result']['_padding'] = '<trimmed>'
            if '_data' in match['result']:
               match['result']['_data'] = zlib.decompress(match['result']['_data'])

         print('MATCH on {}/{}/{}/{}:\n\n    {}\n'.format( match['db_handle']['db_type'],
                                                           match['db_handle']['db_set'],
                                                           match['db_handle']['db_shard'],
                                                           match_type,
                                                           match['result']))

   # Print any errors
   if len(errors) > 0:
      for error in errors:
         print('ERROR: {}'.format(error))


#############################################################
## Extract MSISDN from a CPS subscriber Mongo DB dictionary
def cpsGetSubscriberMsisdn(mongo_row):
   if mongo_row and 'credentials_key' in mongo_row:
      for cred in mongo_row['credentials_key']:
         if cred['type_key'] and cred['type_key'] == 'primary':
            return cred['network_id_key']

#############################################################
## Extract IMSI from a CPS subscriber Mongo DB dictionary
def cpsGetSubscriberMsisdn(mongo_row):
   if mongo_row and 'credentials_key' in mongo_row:
      for cred in mongo_row['credentials_key']:
         if cred['type_key'] and cred['type_key'] == 'IMSI':
            return cred['network_id_key']

#############################################################
## Extract IMSI from a CPS subscriber Mongo DB dictionary
def cpsGetSubscriberSubid(mongo_row):
   if mongo_row and '_id_key' in mongo_row:
      return mongo_row['_id_key']


#############################################################
## Return CPS DB query matches for a subscriber identity
def cpsMongoQuery(db_handle, find, id_type, this_id):

   if args.debug: print('DEBUG: cpsMongoQuery(): Entry: db_handle/db_type={}, find={}, id_type={}, this_id={}'.format(
      db_handle['db_type'], find, id_type, this_id))

   results = []
   mongo_filter = dict()

   # Filter: subscriber / imsi
   if db_handle['db_type'] == 'SPR' and id_type == 'imsi' and find == 'subscriber':
      mongo_filter = {'credentials_key': { '$elemMatch': { 'network_id_key': this_id, 'type_key': 'IMSI'}}}

   # Filter: subscriber / msisdn
   elif db_handle['db_type'] == 'SPR' and id_type == 'msisdn' and find == 'subscriber':
      mongo_filter = {'credentials_key': { '$elemMatch': { 'network_id_key': this_id, 'type_key': 'primary'}}}

   # Filter: subscriber / subid
   elif db_handle['db_type'] == 'SPR' and id_type == 'subid' and find == 'subscriber':
      mongo_filter = {'_id_key': this_id}

   # Filter: apirouter_sk_cache / imsi + msisdn
   elif db_handle['db_type'] == 'SPR' and (id_type == 'imsi' or id_type == 'msisdn') and find == 'apirouter_sk_cache':
      mongo_filter = {'tags': { '$in': [ 'ApiRouterSecondaryKey:networkId:' + this_id ]}}

   # Filter: account / subid
   elif db_handle['db_type'] == 'BALANCE' and id_type == 'subid' and find == 'account':
      mongo_filter = {'subscriberId': this_id}

   # Filter: session / subid
   elif db_handle['db_type'] == 'SESSION' and id_type == 'subid' and find == 'session':
      mongo_filter = {'tags': { '$in': [ 'USuMSubscriberIdKey:usumSubscriberId:' + this_id ]}}

   else:
      print('ERROR: cpsMongoQuery(): Invalid Input: db_handle={}, find={}, id_type={}, this_id={}'.format(db_handle, find, id_type, this_id))

   # Execute the prepared Mongo find
   if mongo_filter:
      if args.debug: print('DEBUG: cpsMongoQuery(): mongo_filter={}'.format(mongo_filter))
      for row in db_handle['db_handle'][find].find(mongo_filter):
         results.append({ 'db_handle': db_handle, 'result': row})

   # Return Results
   if args.debug: print('DEBUG: cpsMongoQuery(): results={}'.format(results))
   return results


#############################################################
## Delete a specified record directly on the Mongo DB
def cpsMongoDelete(db_handle, collection, record_id):

   if not re.match("^[A-Za-z0-9]*$", record_id):
      print('ERROR: cpsMongoDelete(): Unexpected record_id value of {}'.format(record_id))
      return

   if collection == 'account':            mongo_filter = {    '_id': record_id}
   if collection == 'subscriber':         mongo_filter = {'_id_key': record_id}
   if collection == 'apirouter_sk_cache': mongo_filter = {    '_id': record_id}
   if collection == 'session':            mongo_filter = {    '_id': record_id}

   if args.debug: print('DEBUG: cpsMongoDelete(): Delete {}/{}/{}/{}'.format(record_id, collection, db_handle, mongo_filter))

   if args.force or core_yes_or_no("Delete key {} on DB {} (no undo)?".format(record_id, db_handle)):
      db_handle[collection].remove(mongo_filter)

#############################################################
## Open Mongo DB connection handers for CPS environment and return as array
def mongoDbHandlers(mongo_config):

   mongo_db = []

   for db_type in mongo_config:
      for db_set in mongo_config[db_type]:
         if db_type == 'SPR':
            if args.debug: print("DEBUG: mongoDbHandlers(): Opening handle to {}/{}".format(db_type, db_set))
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'spr',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).spr})
         if db_type == 'BALANCE':
            if args.debug: print("DEBUG: mongoDbHandlers(): Opening handle to {}/{}".format(db_type, db_set))
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'balance_mgmt',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).balance_mgmt})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'balance_mgmt_1',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).balance_mgmt_1})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'balance_mgmt_2',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).balance_mgmt_2})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'balance_mgmt_3',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).balance_mgmt_3})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'balance_mgmt_4',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).balance_mgmt_4})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'balance_mgmt_5',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).balance_mgmt_5})
         if db_type == 'SESSION':
            if args.debug: print("DEBUG: mongoDbHandlers(): Opening handle to {}/{}".format(db_type, db_set))
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'session_cache',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).session_cache})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'session_cache_2',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).session_cache_2})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'session_cache_3',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).session_cache_3})
            mongo_db.append({'db_type': db_type, 'db_set': db_set, 'db_shard': 'session_cache_4',
                           'db_handle': MongoClient(mongo_config[db_type][db_set]['ip_addr'], int(mongo_config[db_type][db_set]['port'])).session_cache_4})

   return mongo_db


#############################################################
## Refresh the Mongo configuration cache
def refreshCpsMongoConfig(cfg_file_base, env):

   print('Refreshing Cached Mongo Configuration in {}... '.format(cfg_file_base + env), end='', flush=True)

   # Replace the local cache if the refresh option is set
   mongo_config = core_mongo_diag(args.debug,
                                 [mapper['cps'][env]['cluman']['sjc'], mapper['cps'][env]['cluman']['phx']],
                                  mapper['cps'][env]['username'],
                                  mapper['cps'][env]['password'] )

   with open(cfg_file_base + env, 'w') as mongo_cache_file:
      json.dump(mongo_config, mongo_cache_file, indent=0)

   print('DONE')

#############################################################
## Return the local Mongo CPS configuration cache
def getCpsMongoConfig(cfg_file_base, env):

   if os.path.isfile(cfg_file_base + env):
      try:
         with open(cfg_file_base + env, 'r') as mongo_cache_file:
            mongo_config = json.load(mongo_cache_file)
      except:
         print('ERROR: Specified cfg_file_base {} cannot be read.'.format(cfg_file_base + env))
         sys.exit(1)
      else:
         if args.debug: print("DEBUG: main(): mongo_config={mongo_config}".format(mongo_config=mongo_config))
         return mongo_config
   else:
      print('ERROR: Specified cfg_file_base {} cannot be read.'.format(cfg_file_base + env))
      sys.exit(1)

#############################################################
## Count entries in each CPS Mongo DB
def cpsSchemaCount(mongo_db):

   print("")

   # Subscriber
   print("SPR/subscriber Counts\n==========================")
   counter = 0
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'SPR':
         if args.debug: print('DEBUG: cpsScehmaCount(): Count Execute on SPR/subscriber')
         this_count = db_handle['db_handle'].subscriber.count();
         counter = counter + int(this_count)
         print("  {}: {}".format(db_handle['db_set'], this_count))
   print("  -----------------\n  TOTAL: {}\n".format(counter))

   # Secondary Key Cache
   print("SPR/apirouter_sk_cache Counts\n==========================")
   counter = 0
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'SPR':
         if args.debug: print('DEBUG: cpsScehmaCount(): Count Execute on SPR/apirouter_sk_cache')
         this_count = db_handle['db_handle'].apirouter_sk_cache.count();
         counter = counter + int(this_count)
         print("  {}: {}".format(db_handle['db_set'], this_count))
   print("  -----------------\n  TOTAL: {}\n".format(counter))

   # Balance
   print("BALANCE/account Counts\n==========================")
   counter = 0
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'BALANCE':
         if args.debug: print('DEBUG: cpsScehmaCount(): Count Execute on BALANCE/account')
         this_count = db_handle['db_handle'].account.count();
         counter = counter + int(this_count)
         print("  {}: {}".format(db_handle['db_set'], this_count))
   print("  -----------------\n  TOTAL: {}\n".format(counter))

   # Session
   print("SESSION/session Counts\n==========================")
   counter = 0
   for db_handle in mongo_db:
      if db_handle['db_type'] == 'SESSION':
         if args.debug: print('DEBUG: cpsScehmaCount(): Count Execute on SESSION/session')
         this_count = db_handle['db_handle'].session.count();
         counter = counter + int(this_count)
         print("  {}: {}".format(db_handle['db_set'], this_count))
   print("  -----------------\n  TOTAL: {}\n".format(counter))


#############################################################
## Print the results for a Subscriber
def printSubscriber(label, subscriber_record):

  print("{label:3}: "
        "source={source:5}  "
        "imsi={imsi:15}  "
        "msisdn={msisdn:15}  "
        "policy={policy!s:40}  "
        "alter_speed={alter_speed:5}  "
        "reduce_qos={reduce_qos:5}".format( label=label,
                                           source=subscriber_record.source,
                                             imsi=subscriber_record.imsi,
                                           msisdn=subscriber_record.msisdn,
                                           policy=subscriber_record.policy,
                                      alter_speed=repr(subscriber_record.alter_speed),
                                       reduce_qos=repr(subscriber_record.reduce_qos) ))


#############################################################
## COMPARE RESULTS
def compareSubscriber(src_subscriber, cps_subscriber):

   # Track any errors in a mismatch list.  Any error results in delete/create of that subcriber
   mismatch = []
   
   # msisdn
   if src_subscriber.imsi != cps_subscriber.imsi:
      mismatch.append('imsi (src={}, cps={})'.format(src_subscriber.imsi, cps_subscriber.imsi))

   # msisdn
   if src_subscriber.msisdn != cps_subscriber.msisdn:
      mismatch.append('msisdn (src={}, cps={})'.format(src_subscriber.msisdn, cps_subscriber.msisdn))

   # policy
   if src_subscriber.policy != cps_subscriber.policy:
      mismatch.append('policy (src={}, cps={})'.format(src_subscriber.policy, cps_subscriber.policy))
      
   # alter_speed
   if src_subscriber.alter_speed != cps_subscriber.alter_speed:
      mismatch.append('alter_speed (src={}, cps={})'.format(src_subscriber.alter_speed, cps_subscriber.alter_speed))
      
   # reduce_qos
   if src_subscriber.reduce_qos != cps_subscriber.reduce_qos:
      mismatch.append('reduce_qos (src={}, cps={})'.format(src_subscriber.reduce_qos, cps_subscriber.reduce_qos))
      
   # Return any mis-matches
   return mismatch


#############################################################
## Fix the CPS subscriber via API delete/create
def cpsSubscriberApi(action, cps_api_connection, this_subscriber):

   global service_code_map

   soap_label = 'https://{}:{}/ua/soap'.format(mapper['cps'][args.env]['api'][args.cps], '443')

   ### SimpleSoap no WSDL
   client = SoapClient(
      location = soap_label,
      action = soap_label,
      namespace = soap_label,
      soap_ns='soap',
      trace = args.debug,
      ns = False,
      exceptions=True)

   # Delete
   if action == 'delete':
      if args.debug: print('DEBUG: cpsSubscriberApi(): delete={}'.format(this_subscriber.imsi))

      # Generate XML for delete
      xml = SimpleXMLElement(
       """<?xml version="1.0" encoding="UTF-8"?>
          <se:Envelope xmlns:se="http://schemas.xmlsoap.org/soap/envelope/">
            <se:Body>
            <DeleteSubscriberRequest xmlns="http://broadhop.com/unifiedapi/soap/types">
              <audit>
                <id>cps_audit.py</id>
                <comment>delete imsi {imsi}</comment>
              </audit>
              <networkId>{imsi}</networkId>
              <hardDelete>true</hardDelete>
            </DeleteSubscriberRequest>
            </se:Body>
          </se:Envelope>""".format(imsi=this_subscriber.imsi)
        )

      # Send SOAP Message
      response = client.call('DeleteSubscriberRequest', xml)
      print('FIX: Deleting IMSI {} from CPS.'.format(this_subscriber.imsi))

   # Create
   if action == 'create':
      if args.debug: print('DEBUG: cpsSubscriberApi(): create={}'.format(this_subscriber.imsi))

      # Catch error scenario with the mapping file
      if not this_subscriber.policy or this_subscriber.policy not in service_code_map:
         print('ERROR: cpsSubscriberApi(): Source Policy missing, or not referenced in mapping file: {}'.format(this_subscriber.policy))
         return

      # Include alter_speed AVP if required
      alter_speed_xml=''
      if this_subscriber.alter_speed:
         alter_speed_xml='<avp><code>CCAR_THROTTLE</code><value>{}</value></avp>'.format(this_subscriber.alter_speed)

      # Include alter_speed AVP if required
      reduce_qos_xml=''
      if this_subscriber.reduce_qos:
         reduce_qos_xml='<avp><code>CCAR_QCI</code><value>9</value></avp>'

      # Generate XML for create
      xml = SimpleXMLElement(
       """<?xml version="1.0" encoding="UTF-8"?>
          <se:Envelope xmlns:se="http://schemas.xmlsoap.org/soap/envelope/">
            <se:Body>
            <CreateSubscriberRequest xmlns="http://broadhop.com/unifiedapi/soap/types">
              <audit>
                <id>cps_audit.py</id>
                <comment>create imsi {imsi}</comment>
              </audit>
              <subscriber>
              <name>
                <fullName>{imsi}</fullName>
              </name>
              <credential>
                <networkId>{msisdn}</networkId>
                <type>primary</type>
              </credential>
              <credential>
                <networkId>{imsi}</networkId>
                <type>IMSI</type>
              </credential>
              <service>
                <code>{service}</code>
                <enabled>true</enabled>
              </service>
              <status>ACTIVE</status>
              <avp>
                <code>CCAR_POLICY</code>
                <value>{policy}</value>
              </avp>
              {alter_speed_xml}
              {reduce_qos_xml}
              </subscriber>
            </CreateSubscriberRequest>
            </se:Body>
          </se:Envelope>""".format(imsi=this_subscriber.imsi,
                                 msisdn=this_subscriber.msisdn,
                                service=service_code_map[this_subscriber.policy],
                                 policy=this_subscriber.policy,
                        alter_speed_xml=alter_speed_xml,
                         reduce_qos_xml=reduce_qos_xml)
        )

      # Send SOAP message
      response = client.call('CreateSubscriberRequest', xml)
      print('FIX: Creating IMSI {} on CPS.'.format(this_subscriber.imsi))

#############################################################
## Load the CPS Service Code mapping data
def loadCpsServiceCodeMap(map_file):

   returner = dict()

   with open(map_file, 'rt') as map_file_handle:
      for line in map_file_handle:
         line = line.rstrip()
         line = line.lstrip()
         fields = line.split(',')
         returner.update({fields[0]:fields[1]})
         if args.debug: print('DEBUG: loadCpsServiceCodeMap(): Adding Service Mapping: {} / {}'.format(fields[0], fields[1]))

   return returner

#############################################################
## MAIN LOOP

# Check if this process is running from previous attempt
#core_check_running(args.debug,os.path.basename(__file__))

# Verify Parameters
validate_parameters()

### ROOT: Refresh the local CPS mongo cache data
if args.action == 'cps_cfg_cache':
   refreshCpsMongoConfig(cps_cfg_file_base, args.env)


### ROOT: Oracle SPR Export Action Specified
elif args.action == 'export':

   # CPS
   if args.system == 'cps':
      print("Collecting Cached Mongo State.")
      mongo_config = getCpsMongoConfig(cps_cfg_file_base, args.env)
      exportCps(args.env, args.csvfile, mongo_config)

   # Oracle
   elif args.system == 'oracle':
      exportOracle(args.env, args.csvfile, args.target)


### ROOT: Subscriber query on the DB, with option to delete
elif args.action == 'cpsdb':

   print("Collecting Cached Mongo State.")
   mongo_config = getCpsMongoConfig(cps_cfg_file_base, args.env)
   print("Opening DB Handlers to all found Mongo instances.")
   mongo_db = mongoDbHandlers(mongo_config)

   if args.count:
      cpsSchemaCount(mongo_db)

   else:
      for id_type in sub_list:
         for this_id in sub_list[id_type].copy():

            # Query all DB instances and return matches for this and found assocaited IDs
            if args.debug: print('DEBUG: main(): Call to cpsSchemaPerSub({}, {}).'.format(id_type, this_id))
            db_matches = cpsSchemaPerSub(id_type, this_id, mongo_db)
   
            # Validate the results and print the output
            validation_errors = cpsValidateSubscriber(db_matches)
   
            # Display Found Data
            cpsShowSubscriber(db_matches, validation_errors, args.status)
   
            # Delete items if flag is set
            # Purge from SPR and Secondary Key Cache
            if args.delete and 'spr' in args.delete:
               for match in db_matches['subscriber']:
                  cpsMongoDelete(match['db_handle']['db_handle'], 'subscriber', match['result']['_id_key'])
   
               for match in db_matches['apirouter_sk_cache']:
                  cpsMongoDelete(match['db_handle']['db_handle'], 'apirouter_sk_cache', match['result']['_id'])
   
            # Delete from balance (including if SPR is deleted)
            if args.delete and ('balance' in args.delete or 'spr' in args.delete):
               for match in db_matches['account']:
                  cpsMongoDelete(match['db_handle']['db_handle'], 'account', match['result']['_id'])
   
            # Delete session (including if SPR is deleted)
            if args.delete and ('session' in args.delete or 'spr' in args.delete):
               for match in db_matches['session']:
                  cpsMongoDelete(match['db_handle']['db_handle'], 'session', match['result']['_id'])


### ROOT: Fix Action Specified
elif args.action == 'sync':

   # Load the CPS Service Code mapping file
   service_code_map = loadCpsServiceCodeMap(cps_service_code_map_file)

   # Open a connection to the ADHOC DB (Control Center)
   if args.pod:
      cc_db_connection = cx_Oracle.connect('{}/{}@//vespa:1569/ADHOC'.format(oracle_adhoc_user, oracle_adhoc_pass))
      cc_db_cursor = cc_db_connection.cursor()

   # Open a connection to the Oracle SPR API service
   if args.oracle:
      spr_api_connection = http.client.HTTPConnection(mapper['ora'][args.env]['host'][args.oracle], 8787)

   # Open a connection to the CPS API service
   cps_api_connection = http.client.HTTPSConnection(mapper['cps'][args.env]['api'][args.cps], 443)

   if args.debug: print("DEBUG: main(sync): sub_list['imsi']={}".format(sub_list['imsi']))

   # Itterate over all IMSIs
   for this_imsi in sub_list['imsi']:

      src_result = subscriber()

      # Query Oracle SPR API if configured
      if args.oracle:
         spr_result = querySprApi(spr_api_connection, this_imsi)
         if args.verbose:
            printSubscriber('SPR', spr_result)
         src_result = spr_result

      # Query CC ADHOC DB if configured
      if args.pod:
         cc_result = queryCcDb(cc_db_cursor, args.pod, this_imsi)
         if args.verbose:
            printSubscriber(' CC', cc_result)
         src_result = cc_result

      # Query CPS API
      cps_result = queryCpsApi(cps_api_connection, this_imsi)
      if args.verbose:
         printSubscriber('CPS', cps_result)

      # Compare the results and report discrepencies
      mismatch = []
      if args.recommend:
         mismatch = compareSubscriber(src_result, cps_result)

         # If there is a mismatch identified display
         if mismatch:
            print('ERROR: imsi={}: {}'.format(cps_result.imsi, " ".join(mismatch)))

            # If fix is specified execute the change
            if args.fix:
               if src_result.imsi == '' and cps_result.imsi != '':
                  cpsSubscriberApi('delete', cps_api_connection, cps_result)
               elif src_result.imsi != '' and cps_result.imsi == '':
                  cpsSubscriberApi('create', cps_api_connection, src_result)
               else:
                  cpsSubscriberApi('delete', cps_api_connection, cps_result)
                  cpsSubscriberApi('create', cps_api_connection, src_result)

   # Close all open connections
   if args.pod: cc_db_connection.close()
   if args.oracle: spr_api_connection.close()
   cps_api_connection.close()


### ROOT: Fix Action Specified
elif args.action == 'compare':

   # Read the CPS export into memory
   cps_key = dict()
   with open(args.cps_export, 'rt') as cps_export_handle:
      for line in cps_export_handle:
         fields = line.split(',')
         key = fields[2].split('=')[1]
         value = fields[3] + ',' + fields[4] + ',' + fields[5] + ',' + fields[6]

         cps_key.update({key: value})
         if args.debug: print('DEBUG: Adding CPS key/value: {}/{}'.format(key, value))

   # Read the SPR export and compare as we go
   with open(args.spr_export, 'rt') as spr_export_handle:
      for line in spr_export_handle:
         fields = line.split(',')
         key = fields[2].split('=')[1]
         value = fields[3] + ',' + fields[4] + ',' + fields[5] + ',' + fields[6]
         
         if key in cps_key:
            # Matching entry; clear from cps_key and move on
            if cps_key[key] == value:
               del cps_key[key]
            else:
               print('{}:IMSI found with different parameters.'.format(key))
               del cps_key[key]
         else:
            print('{}:IMSI missing from CPS.'.format(key))

   # any orphan keys are missing from Oracle
   for imsi in cps_key:
      print('{}:IMSI missing from Oracle.'.format(imsi))


# Clean up PID file
#core_check_running(args.debug,os.path.basename(__file__))

sys.exit(0)
