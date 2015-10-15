from __future__ import print_function
import os, sys, time, re, traceback
import json
import logging
import logging.config
from urlparse import urlparse
from httplib import HTTPConnection
import xlrd
from cStringIO import StringIO
import cStringIO
import itertools
from time import localtime, strftime
import getopt, copy
import pymssql
import Queue
import threading
import time

CONST = {"TABLE_TYPES":["COLUMN TABLE", "SEQUENCE TABLE", "SETUP TABLE", "TEARDOWN TABLE"],
         "TEST_ACTION":{"COLUMN TABLE":"TEST", "SEQUENCE TABLE":"TEST", "SETUP TABLE":"SETUP", "TEARDOWN TABLE": "TEARDOWN"},
         "ROW_NAME":"COMMENT",
         "DATA": ["DATA TABLE"],
         "TABLES": ["BigEar", "Dispatcher", "PTT", "PTT2", "QueueOne", "TMOrder"]
         }

def short_exc_info():
    "Return a version of sys.exc_info() with reduced traceback frame"
    exctype, excvalue, tb = sys.exc_info()
    newtb = tb.tb_next
    if newtb is None:
        return (exctype, excvalue, tb)
    return (exctype, excvalue, newtb)

def decode_exc():
    "Separate out the exception and stack trace from an exception"
    (a,b,c) = short_exc_info()
    exception = traceback.format_exception_only(a, b)[0].rstrip('\n')
    trace = ''.join(traceback.format_tb(c))    
    return (exception, '\n' + trace)

def get_time_stamp():
    "return a string 'year month day hour minutes seconds' useful to stamp file names"
    year, month, day, hour, minute, second, weekday, julian, daylight = time.localtime(time.time())
    stamp= "%04d%02d%02d_%02dh%02dm%02d" % (year, month, day, hour, minute, second)
    return stamp

def debug_print(s, end = "\n", file = sys.stdout):
    if test_program.debug_flag: 
        print(s, end = end, file=file)

class TestCaseThread (threading.Thread):
    def __init__(self, thread_id, name, q):
        threading.Thread.__init__(self, name=name)
        self.thread_id = thread_id
        self.name = name
        self.q = q

    def run(self):
        while not exit_flag:
            if not work_queue.empty():
                test = work_queue.get()
                test.stdout = StringIO()
                test.run_test()
                print (test.stdout.getvalue(), end= "")
            time.sleep(1)
        #print ("Thread %s can be terminated"%self.name)
    
class TestCase():

    def __init__(self, test_name, test_type, rows, settings, iterators, setUpRows=[], tearDown=[]):
        self.name = test_name
        self.type = test_type
        self.settings = settings
        self.iterators = iterators
        self.res = {}
        self._res = {"SETUP": [], "TEST":[], "TEARDOWN": []}
        self.actions = {}
        self.actions["SETUP"] = setUpRows
        self.actions["TEST"] = rows
        self.actions["TEARDOWN"] = tearDown
    
    def actions_count(self, type):
        return len(self.actions[type])
    
    def get_action_name(self, type, i):
        for tt in CONST["TABLE_TYPES"]:
            if tt in self.actions[type][i]:
                return self.actions[type][i][tt][CONST["ROW_NAME"]]
    
    def get_name(self):
        return self.name

    def get_id(self, table, tt, is_test):
        const = {"BigEar":      {"PT": {"id":"StrId", "db0":"STLink",       "db1":"STLink"} },
                 "Dispatcher":  {"PT": {"id":"PTTID", "db0":"PaymentTrust", "db1":"PaymentTrustTest"} },
                 "PTT":         {"PT": {"id":"PTTID", "db0":"PaymentTrust", "db1":"PaymentTrustTest"} },
                 "PTT2":        {"PT": {"id":"PTTID", "db0":"PaymentTrust", "db1":"PaymentTrustTest"} },
                 #"APMDetailes": {"PT": {"id":"PTTID", "db0":"PaymentTrust", "db1":"PaymentTrustTest"} },
                 "TMOrder":    {"PT": {"id":"TMOrder", "db0":"PaymentTrust", "db1":"PaymentTrustTest"} },
                 "QueueOne":    {"PT": {"id":"PTTID", "db0":"PaymentTrust", "db1":"PaymentTrustTest"} },
                 }
        test_program.assert_(table in const.keys(), "Please provide id detail for %s table"%table)
        return const[table][tt]["id"], const[table][tt]["db"+is_test]

    def db_connect(self, server, user, password, database, query):
        rtn = [] 
        conn = pymssql.connect(server, user, password, database)
        cursor = conn.cursor(as_dict=True)
        cursor.execute(query)
        row = cursor.fetchone()
        while row:
            rtn.append(row)
            row = cursor.fetchone()
        conn.close()
        return rtn
    
    def send(self, stlink, string_in):
        #test_program.logger.info("StLink: %s"%stlink) 
        test_program.logger.info(string_in)
        urlparts = urlparse(stlink)
        conn = HTTPConnection(urlparts.netloc, urlparts.port or 80)
        conn.request("POST", urlparts.path, string_in)
        resp = conn.getresponse()
        body = resp.read()
        test_program.logger.info("Response: %s"%(body))
        return body
    
    def run_test(self):
        start_time = time.time()
        test_program.tdr.start_test(test_program.tdr.suite_name, self.get_name().encode('utf-8'), file=self.stdout)
        test_program.logger.info("--"*15+"FTEST Run Suite: %s Testcase: %s"%(test_program.tdr.suite_name, self.get_name().encode('utf-8'))+"--"*15)
        try:
            self.run_actions("SETUP")
            self.run_actions("TEST")
            self.run_actions("TEARDOWN")
        except Exception, e:
            debug_print(decode_exc()[1], end = "", file=self.stdout)
            test_program.tdr.end_test(self.get_name().encode('utf-8'),e, file=self.stdout)
            #traceback.print_exc()
        else:
            test_program.tdr.end_test(file=self.stdout)
        stop_time = time.time()
        debug_print("Time: %.3fs" % float(stop_time - start_time), file=self.stdout)    

    def run_actions(self, type):
        for j in range(self.actions_count(type)):
            respond = ""
            stlink, stringIn = self.prepare_string(type ,j)
            debug_print("FTEST %s Action %s"%(type, self.get_action_name(type, j)), file=self.stdout)
            respond = self.send(stlink.encode('utf-8'), stringIn.encode('utf-8'))
            try:
                self.parse_respond(type, j, respond)
                debug_print("\t%s - %s"%(self.get_transact_id(type, j)), end="", file=self.stdout)
                self.find_transaction_in_db(type,j)
                self.execute(type,j)
                #debug_print("", file=self.stdout)
            except AssertionError, e:
                debug_print("\tFAILED %s - %s"%(self.get_transact_id(type, j)), file=self.stdout)
                n=j+1
                while n<self.actions_count(type):
                    debug_print("FTEST Test Action %s NOT EXCECUTED"%self.get_action_name(type, n), file=self.stdout)
                    n+=1
                if type=="SETUP":
                    for i in range(self.actions_count("TEST")):
                        debug_print("FTEST Test Action %s NOT EXCECUTED"%self.get_action_name("TEST", i), file=self.stdout)
                if type=="SETUP" or type=="TEST":
                    for i in range(self.actions_count("TEARDOWN")):
                        debug_print("FTEST Test Action %s NOT EXCECUTED"%self.get_action_name("TEARDOWN", i), file=self.stdout)
                debug_print(decode_exc()[1], end = "", file=self.stdout)
                test_program.assert_(0,self.get_action_name(type, j) + " " + e.message)

    def execute(self, type, act_num):
        sys.path.append("tools") 
        if self.actions[type][act_num].has_key("Execute"):
            for method_full_name in self.actions[type][act_num]["Execute"].keys():
                moduleName, methodName = method_full_name.split(".") 
                if self.actions[type][act_num]["Execute"][method_full_name].startswith("$"):
                    debug_print("\n\t"+method_full_name + " execution", end=" - ", file=self.stdout)
                    _temp = __import__(moduleName, globals(), locals(), [methodName], -1)
                    method = getattr(_temp, methodName)
                    try:
                        res = method()
                        self.res[self.actions[type][act_num]["Execute"][method_full_name][1:]] = res
                        debug_print("OK    OTC - %s"%res, file=self.stdout)
                        test_program.logger.info( "Execution of %s was OK: result %s"%(method_full_name, res) )
                    except Exception, e:
                        test_program.assert_(0, "Execution of %s was failed: %s"%(method_full_name, e))
        else:
            debug_print("", file=self.stdout)

    def find_transaction_in_db(self, type, act_num):
        if self.actions[type][act_num].has_key("DBCheck"):
            if "ServersConfig" in test_program.tdr.workbook.sheet_names():
                #get server name
                server, username, password = test_program.tdr.get_server_info(self._res[type][act_num]["PTTID"], self.actions[type][act_num]["Request"]["TransactionType"],self.actions[type][act_num]["Request"]["RequestType"],self.actions[type][act_num]["Request"]["IsTest"])
                test_program.assert_(server!=None and username!=None and password!=None, "Hited server was not found")
                for table in CONST["TABLES"]:
                    if table in self.actions[type][act_num]["DBCheck"].keys():
                        debug_print("\n\tDB check: " + server+"." + table, end=" - ", file=self.stdout)
                        id, db = self.get_id(table, self.actions[type][act_num]["Request"]["TransactionType"],self.actions[type][act_num]["Request"]["IsTest"])
                        query = "Select *  FROM [%s] where %s=%s"%(table, id, self._res[type][act_num][id]) 
                        
                        #check_res = self.db_connect(server, username, password, db, query)
                        res =  self.db_connect(server, username, password, db, query)
                        test_program.logger.info("DB check: Query %s %s Result: %d record(s) was(were) found: %s"%(server, query, len(res), res))
    
                        if  self.actions[type][act_num]["DBCheck"][table]=="0":
                            test_program.assert_(len(res)==0, "Transaction with %s=%s was found on %s, but not expected to be there"%(id, self._res[type][act_num][id], server))
                        if self.actions[type][act_num]["DBCheck"][table]>="1":
                            test_program.assert_(len(res)==1, "Transaction with %s=%s was not found on %s in %s"%(id, self._res[type][act_num][id], server, table))
                        if  self.actions[type][act_num]["DBCheck"][table]>="2":
                            for key in res[0].keys():
                                #save result from db 
                                if key not in self._res[type][act_num].keys():
                                    #print (key, str(res[0][key]))
                                    self._res[type][act_num][key] = str(res[0][key])
                                #can be ignored
                                else:
                                    test_program.assert_(self._res[type][act_num][key]==res[0][key] or str(self._res[type][act_num][key]).rstrip()==str(res[0][key]).rstrip(), "The value under %s from %s on %s server is '%s', but not equal to '%s'"%(key,table,server, res[0][key], self._res[type][act_num][key]))
                                ####################################################
                                if key in self.actions[type][act_num]["Request"]:
                                    if key.upper() not in ["TIMEOUT", "PTTID", "ACCTNUMBER"]:
                                        test_program.assert_(str(res[0][key])==self.actions[type][act_num]["Request"][key] or (str(res[0][key]).endswith(self.actions[type][act_num]["Request"][key][-4:]) and key == 'AcctNumber'), "%s not equal: %s != %s"%(key, str(res[0][key]), self.actions[type][act_num]["Request"][key]))
                        debug_print("OK", end="\t", file=self.stdout)
                        if  self.actions[type][act_num]["DBCheck"][table]>="3":
                            debug_print("\tSSB check", end=": ", file=self.stdout)
                            check_res = {}
                            for key in res[0].keys():
                                check_res[key.upper()] = res[0][key]
                            list_of_server_group = ["STL"]
                            for group in list_of_server_group:
                                for server2, username, password in test_program.tdr.get_server_group_info(self.actions[type][act_num]["Request"]["TransactionType"]+"_"+group+"_"+"Live" if self.actions[type][act_num]["Request"]["IsTest"]=="0" else "Test"):
                                    
                                    res = []
                                    st = time.time()
                                    debug_print(server2+"." + table, end=" - ", file=self.stdout)
                                    while (len(res)!=1 and time.time()-st<10):
                                        res = self.db_connect(server2, username, password, db, query)
                                    
                                    test_program.assert_(len(res)==1, "SSB Check error: Transaction with %s=%s was not found on %s in %s"%(id, self._res[type][act_num][id], server2, table))
                                    ssb_res = {}
                                    for key in res[0].keys():
                                        ssb_res[key.upper()] = res[0][key]
                                    
                                    for key in check_res.keys():
                                        test_program.assert_(ssb_res.has_key(key), "SSB Check error: %s"%(key))
                                        test_program.assert_(str(check_res[key])==str(ssb_res[key]), "SSB Check error: For the same %s %s, the value under '%s' in the %s table is different in %s and %s. %s not equal to %s"%(id, self._res[type][act_num][id], key, table, server, server2, str(check_res[key]), str(ssb_res[key])))
                                    debug_print("OK", end=", ", file=self.stdout)
                debug_print("", file=self.stdout)
            else:
                debug_print("WARNING: There is no sheet with ServersConfig name in %s"%test_program.tdr.file_name, file=self.stdout)
        
    def prepare_string(self, type, act_num):
        string = "StringIn="
        test_program.assert_("Request" in self.actions[type][act_num], "Does not have any request parameters")
        test_program.assert_("stlink" in [k.lower() for k in self.actions[type][act_num]["Request"].keys()], "No STLink is provided for sending transaction")
        for k in self.actions[type][act_num]["Request"].keys():
            if k.lower() == "stlink":
                test_program.assert_(not self.actions[type][act_num]["Request"][k] == "", "No STLink is provided for sending transaction")
                stlink = self.actions[type][act_num]["Request"][k]
            else:
                try:
                    if self.actions[type][act_num]["Request"][k].startswith("$"):
                        test_program.assert_(self.res.has_key(self.actions[type][act_num]["Request"][k][1:]), "%s was not receive in respond"%(self.actions[type][act_num]["Request"][k][1:])) 
                        self.actions[type][act_num]["Request"][k] = self.res[self.actions[type][act_num]["Request"][k][1:]]

                    v = self.actions[type][act_num]["Request"][k]
                    string +="~%s^%s"%(k, v)
                except AttributeError, e:
                    test_program.error_exit(0,"Error happened in %s test case: Can't parse request variable %s, the value %s have to be a string(text) format"%(self.name, k,self.actions[type][act_num]["Request"][k]))

        return stlink, string

    def parse_respond(self, type, act_num, string_out):
        self._res[type].append(dict([(item.split("^")[0], item.split("^")[1]) for item in string_out.split("~") if item !=""]))
        if "Response" in self.actions[type][act_num].keys():
            for k in self.actions[type][act_num]["Response"].keys():
                if not self.actions[type][act_num]["Response"][k]=="":
                    test_program.assert_(self._res[type][act_num].has_key(k), "Did not receive %s in response"%k)
                    try:
                        if self.actions[type][act_num]["Response"][k].startswith("$"):
                            #save value from response
                            self.res[self.actions[type][act_num]["Response"][k][1:]]=self._res[type][act_num][k]
                        else:
                            #check exact res equals expected
                            test_program.assert_(self.actions[type][act_num]["Response"][k]==self._res[type][act_num][k], "Expected %s = %s, but got %s"%(k, self.actions[type][act_num]["Response"][k], self._res[type][act_num][k]))
                    except AttributeError, e:
                        test_program.error_exit(0,"Error happened in %s test case: Can't parse response variable %s, the value %s have to be a string(text) format"%(self.name, k,self.actions[type][act_num]["Request"][k]))
        else:
            test_program.warning_(0, "No expected respond is provided.")

    def get_transact_id(self, type, act_num):        
        if "PTTID" in self._res[type][act_num].keys():
            return "PTTID", self._res[type][act_num]["PTTID"]
        elif "StrId" in self._res[type][act_num].keys():
            return "StrId", self._res[type][act_num]["StrId"]
        return None, None
    
    def get_iter_names(self):
        "returns a dict of unique iterator keys found in this test"
        rtn = {}
        for t in self.actions.keys():
            for i in range(len(self.actions[t])):
                for k,v in self.actions[t][i]["Request"].items():
                    try:
                        if v.startswith("![") and v.endswith("]!"):
                            name = v[2:-2]
                            test_program.error_exit(name not in rtn, "It is not allowed to have the same iterator (%s) in %s table more then 1 time"%(name, self.name)) 
                            rtn[name]={"actionNum":i,"paramName":k, "type": t}
                    except AttributeError, e:
                        test_program.error_exit(0,"Error happened in %s test case: Can't parse request variable %s, the value %s have to be a string(text) format"%(self.name, k,v))
        return rtn
    
    def replace_macros(self):
        "return the same line of data but with macros replaced from self.settings"
        pat_value = re.compile("(.*)\.(.*)\.(.*)")
        for type in ["SETUP", "TEST", "TEARDOWN"]:
            for i in range(len(self.actions[type])):
                for k,v in self.actions[type][i]["Request"].items():
                    try:
                        if v.startswith("!{") and v.endswith("}!"):
                            mac=v[2:-2]
                            match_value = pat_value.match(mac)
                            
                            if mac in self.settings.keys():
                                self.actions[type][i]["Request"][k] = self.settings[mac]
                            elif mac in test_program.tdr.settings:
                                self.actions[type][i]["Request"][k] = test_program.tdr.settings[mac]
                            elif match_value:
                                table_type = CONST["TEST_ACTION"][match_value.group(1)]
                                action_name = match_value.group(2)
                                value_name = match_value.group(3)
                                change_flag=False
                                for n in range(len(self.actions[table_type])):
                                    if self.get_action_name(table_type, n)==action_name:
                                        test_program.assert_(value_name in self.actions[table_type][n]["Request"], "%s was not found in %s.%s"%(value_name, match_value.group(1), action_name))
                                        self.actions[type][i]["Request"][k] =  self.actions[table_type][n]["Request"][value_name]
                                        change_flag=True
                                        break
                                test_program.assert_(change_flag, "Error happened in %s test case: %s was not found in %s"%(self.name, action_name, match_value.group(1)))
                    except AttributeError, e:
                        test_program.error_exit(0,"Error happened in %s test case: Can't parse request variable %s, the value %s have to be a string(text) format"%(self.name, k,v))
        pat_table = re.compile("(.*)\.(.*)\((.*)\)")
        for type in ["SETUP", "TEST", "TEARDOWN"]:
            for i in range(len(self.actions[type])):
                for k,v in self.actions[type][i]["Request"].items():
                    try:
                        if v.startswith("!{") and v.endswith("}!"):
                            mac=v[2:-2]
                            match_table = pat_table.match(mac)
                            
                            if match_table:
                                data_table_name = match_table.group(1)
                                field_name = match_table.group(2)
                                match_name = match_table.group(3)
                                test_program.assert_(match_name in self.actions[type][i]["Request"].keys(), "%s is not found in Request of %s table of %s test case"%(match_name, type, self.name))
                                test_program.assert_(test_program.tdr.data.has_key(data_table_name), "Data table with %s name was not found"%data_table_name)
                                test_program.assert_(match_name in test_program.tdr.data[data_table_name][0], "Data table with %s name does not consist %s field"%(data_table_name, match_name))
                                test_program.assert_(field_name in test_program.tdr.data[data_table_name][0], "Data table with %s name does not consist %s field"%(data_table_name, field_name))
                                j=1
                                change_flag=False
                                while j<len(test_program.tdr.data[data_table_name]):
                                    if dict(zip(test_program.tdr.data[data_table_name][0], test_program.tdr.data[data_table_name][j]))[match_name]==self.actions[type][i]["Request"][match_name]:
                                        self.actions[type][i]["Request"][k] = dict(zip(test_program.tdr.data[data_table_name][0], test_program.tdr.data[data_table_name][j]))[field_name]
                                        change_flag=True
                                        break
                                    j+=1
                                test_program.assert_(change_flag, "%s was not found in %s"%(self.actions[type][i]["Request"][match_name], mac))
                            else:
                                test_program.error_exit(0, "Macro is not found: "+mac)
                    except AttributeError, e:
                        test_program.error_exit(0,"Error happened in %s test case: Can't parse request variable %s, the value %s have to be a string(text) format"%(self.name, k,v))
            
    def fetch_test_with_iterators(self):
        rtn = []
        names_iters = self.get_iter_names()
        # check to prevent explosion
        total_testcases = 1
        for n in names_iters.keys():
            test_program.error_exit(n in self.iterators.keys(), "Iterator '%s' not found" %n)
            total_testcases *= len(self.iterators[n])
        test_program.error_exit(total_testcases <= 1000, "No more than 1000 testcases allowed from the same test table. Check iterators: %s"% names_iters.keys())
        if total_testcases>1:
            nb_iters = len(names_iters)
            test_program.error_exit(nb_iters <=3, "No more than 3 iterators allowed in one test")
            if nb_iters == 1 :
                all_configs =   [
                                    { names_iters.keys()[0]:x }
                                    for x in self.iterators[names_iters.keys()[0]]
                                ]
            elif nb_iters == 2:
                all_configs =   [
                                    { names_iters.keys()[0]:x, names_iters.keys()[1]:y }
                                    for x in self.iterators[names_iters.keys()[0]]
                                    for y in self.iterators[names_iters.keys()[1]]
                                ]
            elif nb_iters == 3:
                all_configs =   [
                                    { names_iters.keys()[0]:x, names_iters.keys()[1]:y, names_iters.keys()[2]:z }
                                    for x in self.iterators[names_iters.keys()[0]]
                                    for y in self.iterators[names_iters.keys()[1]]
                                    for z in self.iterators[names_iters.keys()[2]]
                                ]
            for local_iter_dict in all_configs:
                #curr_test = TestCase(self.name, self.type, self.actions["TEST"], self.settings, self.iterators, self.actions["SETUP"], self.actions["TEARDOWN"])
                curr_test = copy.deepcopy(self)
                for k,v in local_iter_dict.items():
                    curr_test.actions[names_iters[k]["type"]][names_iters[k]["actionNum"]]["Request"][names_iters[k]["paramName"]]=v
                    curr_test.name=curr_test.name+"; "+k+"="+v
                rtn.append(curr_test)
        else:
            rtn.append(self)
        return rtn    

class TestStory():
    def __init__(self, filename=None, sheetname=None):
        "construct the instance; parses the test story"
        self.file_name = os.path.normpath(filename)
        self.sheet_name = sheetname
        self.workbook = xlrd.open_workbook(self.file_name, on_demand = True)
        test_program.error_exit(self.sheet_name in self.workbook.sheet_names(), "FTEST ERROR: There is no sheet with '%s' name in %s book"%(self.sheet_name, self.file_name))
        self.worksheet = self.workbook.sheet_by_name(self.sheet_name)
        self.get_server_config()
        self.suite_name = None
        self._tests = []
        self.suite = None
        self.settings = {}
        self.data = {}
        self._fail = 0
        self._fail_list = []
        self._ok = 0
        self._total = 0

    def add_test(self, test):
        self._tests.append(test)

    def get_server_config(self):
#        test_program.assert_("ServersConfig" in self.workbook.sheet_names(), "There is no sheet with Config name in %s"%self.file_name)
        if "ServersConfig" in self.workbook.sheet_names():
            self.server_info = {}
            configsheet = self.workbook.sheet_by_name("ServersConfig")
            i=0
            while i in range(configsheet.nrows):
                if configsheet.cell(i, 0).value.upper() == "SERVER":
                    type_line = i
                elif configsheet.cell(i, 0).value != xlrd.empty_cell.value:
                    for j in range(self.worksheet.ncols):
                        if configsheet.cell(type_line, j).value.upper() == "SERVER" and configsheet.cell(i, j).value != xlrd.empty_cell.value:
                            sever_name = configsheet.cell(i, j).value
                            if sever_name not in self.server_info.keys():
                                self.server_info[sever_name] = {"USER": None, "PASSWORD":None, "COMPONENT": {}}
                        elif configsheet.cell(i, j).value != xlrd.empty_cell.value and configsheet.cell(type_line, j).value.upper() in ["USER", "PASSWORD"]:
                            self.server_info[sever_name][configsheet.cell(type_line, j).value.upper()] = configsheet.cell(i, j).value
                        elif configsheet.cell(i, j).value != xlrd.empty_cell.value and configsheet.cell(type_line, j).value.upper() == "COMPONENT":
                            assert configsheet.cell(type_line, j+1).value.upper() == "DATABASE" and configsheet.cell(i, j+1).value != xlrd.empty_cell.value, "Data Base name is not provided"
                            self.server_info[sever_name]["COMPONENT"][configsheet.cell(i, j).value] = configsheet.cell(i, j+1).value
                            break
                i+=1
            
    def init_from_file(self):
        in_table = False
        current_test_name = None
        current_test_type = None
        set_up_rows = []
        tear_down_rows = []
        rows = []
        loc_settings = {}
        loc_iterators = {}
        i = 0

        while i in range(self.worksheet.nrows):

            #test suite started   
            if self.worksheet.cell(i, 0).value.upper() == "SUITE":
                test_program.error_exit(self.worksheet.cell(i, 1).value != xlrd.empty_cell.value, "Suite name is not specified")
                self.suite_name = self.worksheet.cell(i, 1).value

            #test case started      
            elif self.worksheet.cell(i, 0).value.upper() == "TEST":
                test_program.error_exit(self.suite_name!= None, "Suite name is not specifyed")
                test_program.error_exit(self.worksheet.cell(i, 1).value != xlrd.empty_cell.value, "Test case name is not specified")
                current_test_name = self.worksheet.cell(i, 1).value
   
            #data table found    
            elif self.worksheet.cell(i, 0).value.upper()=="DATA TABLE":
                data_table_name = self.worksheet.cell(i, 1).value
                self.data[data_table_name] = []
                i+=1
                while self.worksheet.cell(i, 0).value != xlrd.empty_cell.value:
                    line = []
                    for j in range(self.worksheet.ncols):
                        if self.worksheet.cell(i, j).value != xlrd.empty_cell.value:
                            line.append(self.worksheet.cell(i, j).value)
                        else:
                            break
                    i+=1    
                    self.data[data_table_name].append(tuple(line))
            
            #global settings or global iterators
            elif self.worksheet.cell(i, 0).value != xlrd.empty_cell.value and not in_table and current_test_name==None:
                test_program.error_exit(self.suite_name!= None, "Suite name is not specifyed")
                values = []
                for j in range(self.worksheet.ncols):
                    if j!=0 and  self.worksheet.cell(i, j).value != xlrd.empty_cell.value:
                        values.append(self.worksheet.cell(i, j).value)
                if len(values)==1:
                    self.settings[self.worksheet.cell(i, 0).value] = values[0] 
                elif len(values)>1:
                    test_program.warning_(0, "We are currently not support global iterators")
        
            #table found    
            elif self.worksheet.cell(i, 0).value.upper() in CONST["TABLE_TYPES"]:
                test_program.error_exit(current_test_name, "Test table is found, but test case name is not specified previously")
                current_test_type = self.worksheet.cell(i, 0).value.upper()
                type_line = i
                atrubut_name_line = i+1
                in_table = True
                i+=1
              
            #local settings or local iterators
            elif self.worksheet.cell(i, 0).value != xlrd.empty_cell.value and not in_table and current_test_name:
                values = []
                for j in range(self.worksheet.ncols):
                    if self.worksheet.cell(i, j).value == xlrd.empty_cell.value:
                        break
                    elif j!=0 and  self.worksheet.cell(i, j).value != xlrd.empty_cell.value:
                        values.append(self.worksheet.cell(i, j).value)
                if len(values)==1:
                    loc_settings[self.worksheet.cell(i, 0).value] = values[0] 
                elif len(values)>1:
                    loc_iterators[self.worksheet.cell(i, 0).value] = values

            #action in Column or Sequence table or SetUp or TearDown            
            elif self.worksheet.cell(i, 0).value != xlrd.empty_cell.value and in_table and current_test_name:
                act={}
                for j in range(self.worksheet.ncols):
                    if self.worksheet.cell(atrubut_name_line, j).value != xlrd.empty_cell.value and self.worksheet.cell(i, j).value != xlrd.empty_cell.value:
                        if self.worksheet.cell(type_line, j).value not in act.keys():
                                act[self.worksheet.cell(type_line, j).value] = {}
                        act[self.worksheet.cell(type_line, j).value][self.worksheet.cell(atrubut_name_line, j).value] = (self.worksheet.cell(i, j).value)
                rows.append(act)
                if current_test_type == "COLUMN TABLE":
                    test = TestCase(current_test_name + ";" + act[current_test_type][CONST["ROW_NAME"]], current_test_type, rows, loc_settings, loc_iterators, set_up_rows, tear_down_rows) 
                    tests_list = test.fetch_test_with_iterators()
                    for t in tests_list:
                        t.replace_macros()
                        self.add_test(t)
                    rows = []    
                    
            elif self.worksheet.cell(i, 0).value == xlrd.empty_cell.value and in_table:
                if current_test_type == "SEQUENCE TABLE":
                    test = TestCase(current_test_name, current_test_type, rows, loc_settings, loc_iterators) 
                    tests_list = test.fetch_test_with_iterators()
                    for t in tests_list:
                        t.replace_macros()
                        self.add_test(t)
                if current_test_type == "SETUP TABLE":
                    set_up_rows = rows
                    in_table = False
                    current_test_type = None
                    rows = []
                elif current_test_type == "TEARDOWN TABLE":
                    tear_down_rows = rows
                    in_table = False
                    current_test_type = None
                    rows = []
                else:
                    set_up_rows = []
                    tear_down_rows = []
                    in_table = False
                    current_test_name = None
                    current_test_type = None
                    rows = []
                    loc_settings = {}
                    loc_iterators = {}
            i+=1
        if in_table and current_test_name:
            if current_test_type == "SEQUENCE TABLE":
                test = TestCase(current_test_name, current_test_type, rows, loc_settings, loc_iterators) 
                tests_list = test.fetch_test_with_iterators()
                for t in tests_list:
                    t.replace_macros()
                    self.add_test(t)
            if current_test_type == "SETUP TABLE":
                set_up_rows = rows
                in_table = False
                current_test_type = None
                rows = []
            elif current_test_type == "TEARDOWN TABLE":
                tear_down_rows = rows
                in_table = False
                current_test_type = None
                rows = []
            else:
                set_up_rows = []
                in_table = False
                current_test_name = None
                current_test_type = None
                rows = []
                loc_settings = {}
                loc_iterators = {}

    def get_server_info(self, pttid, tt,rt,isTest):
            comp = tt
            if tt=="PT" and rt in ["A", "G", "S", "B"]:
                comp += "_TRX"
            elif tt=="PT" and rt in ["C", "D", "R", "F", "U"]:
                comp += "_STL"
            else:
                test_program.assert_(0, "We do not support any db check yet except PT")
            
            if isTest == "0":
                comp += "_Live"
                if len([c for s in self.server_info.keys() for c in self.server_info[s]["COMPONENT"].keys() if c.startswith(comp)])>1:
                    comp += "_" + str(int(pttid)%10)
            elif isTest == "1":
                comp += "_Test"
                if len([c for s in self.server_info.keys() for c in self.server_info[s]["COMPONENT"].keys() if c.startswith(comp)])>1:
                    comp += "_" + str(int(pttid)%10)
                
            for k in  self.server_info.keys():
                if self.server_info[k]["COMPONENT"].has_key(comp):
                    return k, self.server_info[k]["USER"], self.server_info[k]["PASSWORD"] 
            return None, None, None
    def get_server_group_info(self, group):
        rtn = []
        for k in  self.server_info.keys():
            for comp in self.server_info[k]["COMPONENT"].keys():
                if comp.startswith(group):
                    rtn.append((k, self.server_info[k]["USER"], self.server_info[k]["PASSWORD"]))
        test_program.assert_(rtn!=[], "Any server from %s group was not found in ServersConfig sheet"%(group))
        return rtn
    
    #def printNumberedErrors(self):
    #    if self._fail==0: return
    #    if self._fail == 1:
    #        print( "There was 1 failure:"  )
     #   else:
     #       print( "There were %i failures:" %self._fail)
     #   i = 1
     #   for test,error in self._fail_list:
     #       print ("%i. %s\n   %s" % (i, test, error))
     #       i = i + 1
            
    def report(self, t):
        print( "=="*50)
        if self._fail>0:
            print ("\nFTEST Testsuite has failures\n")
        else:
            print ("\nFTEST Testsuite was OK\n")
        debug_print("Ran %d test%s in %.3fs\n" %(self._total, self._total > 1 and "s" or "", t))
        print ("=-"*50)
        print ("\nFTEST Session summary:\n")
        print ("Suite name " + self.suite_name)
        print ("Number of FAILED testcases  %d"%self._fail)
        print ("Number of OK testcases     %d"%self._ok)
        print ("Number of TOTAL  testcases %d"%self._total)
        print ("")
        print ("=-"*50)
        #self.PRINTNumberedErrors()
        
    def start_test(self, suite_name, test_name, file = sys.stdout):
        print ("--"*50, file=file)
        print ("", file=file)
        print ("FTEST Run Suite: %s Testcase: %s"%(suite_name, test_name), file=file)
        print ("", file=file)
        
    def end_test(self, test_name=None, error = None, file = sys.stdout):
        self._total+=1
        debug_print("", file=file)
        if error and test_name:
            print( "FTEST Testcase FAILED: %s\n"% error, file=file)
            self._fail_list.append((test_name,error))
            self._fail+=1
        else:
            print( "FTEST Testcase OK\n", file=file)
            self._ok+=1

    def check_tables(self, msg):
        if self._tests==[]:
            test_program.usage_exit(msg)        
       
    def filter_out_tests(self):
        "changes in place the suite to remove tests as decided by the command line parameters -f, -F and -t"
        # for the -f/-F/-T features
        if test_program.testcase_filter_t:
            # now filtering
            self._tests =[ tt for tt in self._tests if tt.get_name().encode('utf-8') == test_program.testcase_filter_t ]
            self.check_tables("There is no test with name '%s'" %test_program.testcase_filter_t)
        elif test_program.testcase_filter_match and test_program.testcase_filter_no_match:
            pat_1 = re.compile(test_program.testcase_filter_match)
            pat_2 = re.compile(test_program.testcase_filter_no_match)

            # now filtering
            self._tests = [ tt for tt in self._tests if (
                                                (pat_1.search(tt.get_name().encode('utf-8'))) and
                                                (not pat_2.search(tt.get_name().encode('utf-8')))
                            )]
            self.check_tables("There is no test with name  matching '%s' and not matching '%s'" %(test_program.testcase_filter_match, test_program.testcase_filter_no_match))
        elif test_program.testcase_filter_match or test_program.testcase_filter_no_match:
            if test_program.testcase_filter_match:
                pat = re.compile(test_program.testcase_filter_match)
            else:
                pat = re.compile(test_program.testcase_filter_no_match)
            anti = test_program.anti_filter
            # now filtering
            self._tests = [ tt for tt in self._tests if (
                                                (not anti and pat.search(tt.get_name().encode('utf-8'))) or
                                                (not pat.search(tt.get_name().encode('utf-8')) and anti)
                            )]
            self.check_tables("There is no test with name  matching '%s' or not matching '%s'" %(test_program.testcase_filter_match, test_program.testcase_filter_no_match))

    def run_suite(self):
        start_time = time.time()
        test_program.error_exit(self.suite_name!= None, "Suite was created previously")
        print( "=="*50)
        print("")
        print("FTEST Running Testsuite: " + self.file_name + " " + self.sheet_name + strftime(" on %a, %d %b %Y %H:%M:%S", localtime()))
        print("")
        test_program.logger.info("**"*50) 
        test_program.logger.info("FTEST Running Testsuite from %s"%(self.file_name+" " + self.sheet_name))   
        self.filter_out_tests()  

        if(test_program.number_of_threads>1):
      
            global queue_lock
            global work_queue
            global exit_flag
            exit_flag = 0
            queue_lock = threading.Lock()
            work_queue = Queue.Queue(len(self._tests))
            threads = []
            number_of_threads = test_program.number_of_threads if test_program.number_of_threads<=len(self._tests) else len(self._tests)
            debug_print("FTEST Status: %d tests will be run in %d threads"%(len(self._tests), number_of_threads)) 
            for th in range(number_of_threads):
                tName = "Thread-"+ str(th+1)
                thread = TestCaseThread(th+1, tName, work_queue)
                thread.start()
                threads.append(thread)
            queue_lock.acquire()
            for i in range(len(self._tests)):
                work_queue.put(self._tests[i])
            queue_lock.release()
            while not work_queue.empty():
                pass
            exit_flag = 1
            for t in threads:
                t.join()
        else:
            for i in range(len(self._tests)):
                self._tests[i].stdout = sys.stdout
                self._tests[i].run_test()
            
        stop_time = time.time()
        self.report(float(stop_time - start_time))

class DefaultOptions:
    "small class to get the defaults"
    def __init__(self):
        # default settings
        self.debug_flag = 0
        self.log_object = None
        self.log_asked_by_user = 0
        self.base_directory = os.getcwd()
        self.log_dir = ""
        self.log_file=""
        self.timestamp = "%s_" % get_time_stamp()
        self.testcase_filter_match = None
        self.testcase_filter_no_match = None
        self.testcase_filter_t = None # used to run only 1 single test
        self.anti_filter = 0
        self.explicit_test_module = None
        self.explicit_test_sheet = None
        self.number_of_threads = 0
        

class TestProgramParam(DefaultOptions):
    """A command-line program that runs a set of tests. This is primarily
       for making test modules conveniently executable. It is also used as
       a structure variable to carry the command line parameters to suite
       constructors and testcase constructors
    """
    USAGE=\
"""
%(prog_name)s : a module of the FTEST FRAMEWORK
Usage:
    python %(prog_name)s
                [-b <file name>] [-s <sheet name>]
                [-t <test name>]
                [-d] [-h] [-f|-F <pattern>]
                [-L <log_dir>] [-l <log_file>]

Which testsuites to run:
        -b <file name> : run testcases from test xls file <file name>
        -s <sheet name> : run testcases from test sheet
        -t <name> : only run testcase with specified name
        
        -f <pattern> : only run testcases with name matching pattern
        -F <pattern> : only run testcases NOT matching pattern

Run modes:
        -r <number of threads> : run test cases in number of threads
        -d : activate debug mode (increase the verbosity of other modes)
        -h : help information

Logging:
        -l <log_file> : redirect the output from the testcases
                (not the FTEST report) to the given file
        -L <dir_name> : specify a directory for the log file
                and the summary file (default: current directory);
                if no -l given, it builds a file name from the command line
                module and use it as log_file
"""
    def __init__(self ):
        sys.path.append(os.path.dirname(os.path.abspath(sys.argv[0])))
        self.argv = sys.argv
        self.prog_name = os.path.basename(self.argv[0])
        # default settings
        DefaultOptions.__init__(self)
        self.process_options(sys.argv)
        self.init_log()
        self.logger = logging.getLogger(__name__)
        
    def check_file(self, file_name):
        "check if file is readable"
        if os.access(file_name, os.R_OK):
            return 1
        else:
            return 0
        
    def process_options(self, argv):
        "process command line arguments"
        # read options
        option_string = "b:s:t:dhf:F:o:l:L:r:"
        try:
            Options,Args = getopt.getopt(self.argv[1:],option_string,[])
        except getopt.error, msg:
            self.usage_exit(msg)
        if len(Args)>0:
            # here process TDR in a future version
            print( "FTEST Warning: explicit arguments given: \"%s\"" % Args)
        for Option in Options:
            if Option[0]=='-b':
                self.explicit_test_module = Option[1]
                if not self.check_file(self.explicit_test_module):
                    self.usage_exit("Test module not found: %s" % self.explicit_test_module)
            elif Option[0]=='-s':
                self.explicit_test_sheet = Option[1]
            elif Option[0]=='-t':
                self.testcase_filter_t = Option[1]
                self.anti_filter=0
                print ("FTEST Status: only running testcase with instance name:", self.testcase_filter_t)
            elif Option[0]=='-d':
                self.debug_flag = 1
                print ("FTEST Status: verbose mode on")
            elif Option[0]=='-h':
                self.usage_exit()
            elif Option[0]=='-f':
                self.testcase_filter_match = Option[1]
                self.anti_filter=0
                print ("FTEST Status: only running testcases with instance name matching:", self.testcase_filter_match)
            elif Option[0]=='-F':
                self.testcase_filter_no_match = Option[1]
                self.anti_filter=1
                print ("FTEST Status: running testcases with instance name NOT matching:", self.testcase_filter_no_match)
            elif Option[0]=='-l':
                self.log_file = Option[1]
                self.log_asked_by_user = 1
            elif Option[0]=='-L':
                self.log_asked_by_user = 1
                self.log_dir = Option[1]
            elif Option[0]=='-r' and Option[1].isdigit() :
                self.number_of_threads=int(Option[1])
        if not self.explicit_test_module:
            self.usage_exit("Test module is not specified")        
        if not self.explicit_test_sheet:
            self.usage_exit("Test sheet in module %s is not specified"% self.explicit_test_module)        

    def error_exit(self, expr, msg=None):
        if not expr:
            self.logger.info("FTEST ERROR: %s"%msg)
            print ("FTEST ERROR: %s"%msg)
            sys.exit(2)
        
    def assert_(self, expr, msg=None):
        if not expr:
            self.logger.info("FTEST ASSERT: %s"%msg)
            raise AssertionError, msg

    def warning_(self, expr, msg=None):
        if not expr:
            self.logger.info("FTEST WARNING: %s"%msg)
            print ("FTEST WARNING: %s"%msg)

    def usage_exit(self, msg=None):
        print (self.USAGE % self.__dict__)
        if msg: print ("FTEST ERROR: %s"%msg)
        sys.exit(2)
 
    def init_log( self, default_path='logging.json', 
                 default_level=logging.INFO,
                 env_key='LOG_CFG'
                ):
        """Setup logging configuration"""
        path = default_path
        value = os.getenv(env_key, None)

        if self.log_asked_by_user:
            if self.log_file !="" and self.log_dir!="":
                if os.path.basename(self.log_file)==self.log_file:
                    self.log_file = self.log_dir +os.sep+ self.log_file
            elif self.log_dir !="":
                short_name = os.path.basename(self.explicit_test_module).rsplit(".", 1)[0]+ "_" + self.explicit_test_sheet
                self.log_file = self.log_dir +os.sep+ self.timestamp + short_name + ".log"
        else:
            short_name = os.path.basename(self.explicit_test_module).rsplit(".", 1)[0]+ "_" + self.explicit_test_sheet
            self.log_file = self.base_directory +os.sep+ self.timestamp + short_name + ".log"
            
        try:
            file=open(self.log_file, "a")
            print ("FTEST Status: testcases output directed to file: %s"%self.log_file)                    
        except IOError, msg:
            print ("FTEST ERROR: can not open log file for writing: %s"%self.log_file)
            sys.exit(2)

        if value:
            path = value
        if os.path.exists(path):
            with open(path, 'rt') as f:
                config = json.load(f)
                config[u"handlers"][u"info_file_handler"][u"filename"] = self.log_file
            logging.config.dictConfig(config)
        else:
            logging.basicConfig(level=default_level)

    def run(self):
        self.assert_(self.explicit_test_module and self.explicit_test_sheet, "Excel file name or sheet name with test story is not provided")
        self.tdr = TestStory(self.explicit_test_module, self.explicit_test_sheet)
        self.tdr.init_from_file()
        self.tdr.run_suite()

if __name__ == '__main__':
    test_program = TestProgramParam()
    test_program.run()