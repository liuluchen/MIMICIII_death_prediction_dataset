import sys
import os
import datetime
import time
import re
import logging
import numpy as np
import commands
try:
    from pg import DB
except ImportError:
    sys.stderr.write('can\'t imprt module pg\n')
import argparse


def connect():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host')
    parser.add_argument('--user')
    parser.add_argument('--passwd')
    parser.add_argument('--schema', default = 'mimiciii')
    parser.add_argument('--mode', default = 'all')
    args = parser.parse_args()

    # host = '162.105.146.246'
    # host = 'localhost'
    # schema = 'mimiciii'
    
    host = args.host
    user = args.user
    passwd = args.passwd
    logging.info('connect to %s, user = %s, search_path = %s' %(host, user, args.schema))
    db = DB(host = host, user = user, passwd = passwd)
    db.query('set search_path to %s' %(args.schema))
    return db

class Patient():
    bs_attrs = []

    def __init__(self, row):
        self.values = {}
        self.names = []
        for field in Patient.bs_attrs:
            self.values[field] = row[field]
            self.names.append(field)

    def to_row(self):
        ret = []
        for name in self.names:
            ret.append(self.values[name])
        return ret
        
    @staticmethod
    def set_attrs(columns):
        Patient.bs_attrs = []   
        for field in columns:
            Patient.bs_attrs.append(field)

    @staticmethod
    def write_to_local(patients, path):
        columns = None
        data = []
        index = []
        for pid, patient in patients.iteritems():
            if columns is None:
                columns = patient.names
            data.append(patient.to_row())
            index.append(pid)
        from pandas import DataFrame
        dt = DataFrame(data = data, index = index, columns = columns)
        dt.sort_index()
        dt.to_csv(path)

def date2str(date):
    return date.strftime('%Y-%m-%d')

def time2str(time):
    return time.strftime('%Y-%m-%d %H:%M:%S')

def time_format_str(time):
    return '{0.year:4d}-{0.month:02d}-{0.day:02d} {0.hour:02d}:{0.minute:02d}:{0.second:02d}'.format(time)

def parse_time(time_str):
    if len(time_str) in [18, 19]:
        try:
            return datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        except Exception, e:
            return None
    elif len(time_str) == 10:
        try:
            return datetime.datetime.strptime(time_str, '%Y-%m-%d')
        except Exception, e:
            return None
    elif len(time_str) in [12, 13, 14]:
        try:
            return datetime.datetime.strptime(time_str, '%m/%d/%y %H:%M')
        except Exception, e:
            return None
    elif len(time_str) == 16:
        try:
            return datetime.datetime.strptime(time_str, "%Y/%m/%d %H:%M")
        except Exception, e:
            return None
    return None

def parse_number(number_str):
    try:
        return float(number_str)
    except Exception, e:
        return None

def is_time(time_str):
    time = parse_time(time_str)
    return time is not None

def is_number(number_str):
    number = parse_number(number_str)
    return number is not None

def load_reg(filepath):
    regs = []
    for line in file(filepath):
        line = line.strip()
        if line.startswith("#"):
            continue
        if line == "":
            continue
        regs.append(re.compile(line))
    return regs

def load_id2event_value():
    ret = {}
    for line in file(os.path.join(result_dir, "event_des_text.tsv")):
        parts = line.strip("\n").split(" ")
        event_id = int(parts[0])
        event_type = parts[1]
        value = " ".join(parts[2:])
        ret[event_id] = event_type + '.' + value
    return ret

# def load_id2event_rtype():
#     ret = {}
#     for line in file(os.path.join(result_dir, "event_des_text.tsv")):
#         parts = line.strip("\n").split(" ")
#         event_id = int(parts[0])
#         event_type = parts[1]
#         value = " ".join(parts[2:])
#         ret[event_id] = event_type
#     return ret

def load_numpy_array(filepath):
    return np.load(filepath)

def load_items(filepath):
    items = {}
    for line in file(filepath):
        line = line.strip()
        if line == "":
            continue
        p = line.split('\t')
        code = int(p[0])
        if len(p) == 1:
            des = ""
        else:
            des = p[1]
        items[code] = des
    return items

def get_nb_lines(filepath):
    output = commands.getoutput('wc -l %s' %filepath)
    p = int(output.split(" ")[0])
    return p

def get_nb_files(pattern):
    output = commands.getoutput("ls %s|wc -l" %pattern)
    return int(output)

def set_logging():
    format = '[%(asctime)s] %(filename)-15s[line:%(lineno)-4d] %(levelname)s %(message)s'
    logging.basicConfig(
                level=logging.DEBUG,
                format=format,
                datefmt='%Y-%m-%d %H:%M:%S',
                stream=sys.stdout)

def is_admit(event):
    global admit
    return event.eid in admit

def is_emerg_admit(event):
    global emerg_admit
    return event.eid in emerg_admit

def is_disch(event):
    global disch
    return event.eid in disch

def is_icu_in(event):
    global icu
    return event.eid in icu

def is_icu_leave(event):
    global icu_leave
    return event.eid in icu_leave


original_data_dir = 'mimic_data/original'
static_data_dir = 'mimic_data/static_data'
event_dir = 'mimic_data/event'
stat_dir = 'mimic_data/stat'
result_dir = 'mimic_data/result'
death_exper_dir = 'mimic_data/death_dataset'
config_dir = 'data_process/config'



admit_text = ["ELECTIVE", "EMERGENCY", "NEWBORN", "URGENT"]
admit = [2, 3, 4, 5]
emerg_admit = [3, 5]
disch = [6]
icu = [7]
death = [2371]
icu_leave = [3418]
black_list = [2371]


set_logging()