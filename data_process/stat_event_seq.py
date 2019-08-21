from util import *
from patient import SimpleEvent
import json

class PatientCnt:
    def __init__(self, pid):
        self.pid = pid
        self.nb_hospital = 0
        self.now_hospital_cnt = None
        self.hospital_cnts = []

    def count(self, event):
        if is_admit(event):
            self.nb_hospital += 1
            self.now_hospital_cnt = HospitalCnt(is_emerg_admit(event), event.time)
            self.hospital_cnts.append(self.now_hospital_cnt)
        elif is_disch(event):
            self.now_hospital_cnt.close(event.time)
            self.now_hospital_cnt = None
        elif self.now_hospital_cnt is not None:
            self.now_hospital_cnt.count(event, self.pid)

    def json_obj(self):
        return {
            "pid": self.pid,
            "nb_hospital": self.nb_hospital,
        }

    @staticmethod
    def load_from_json(obj):
        pid = obj['pid']
        patient_cnt = PatientCnt(pid)
        patient_cnt.nb_hospital = obj['nb_hospital']
        patient_cnt.hospital_cnts = []
        return patient_cnt

    def write(self, writer):
        out_str = json.dumps(self.json_obj())
        writer.write(out_str + "\n")
        for hospital_cnt in self.hospital_cnts:
            hospital_cnt.write(writer)
        
    @staticmethod
    def load(reader):
        line = reader.readline()
        if line == "":
            return None
        json_obj = json.loads(line)
        patient_cnt = PatientCnt.load_from_json(json_obj)
        for i in range(patient_cnt.nb_hospital):
            line = reader.readline()
            hos_json_obj = json.loads(line.strip())
            patient_cnt.hospital_cnts.append(HospitalCnt.load_from_json(hos_json_obj))
        return patient_cnt

    def stat(self):
        for hospital_cnt in self.hospital_cnts:
            hospital_cnt.stat()


class HospitalCnt:
    def __init__(self, is_emergency, admit_time):
        self.admit_time = admit_time
        self.nb_icu = 0
        self.nb_event = 0
        self.is_emergency = is_emergency
        self.now_icu_cnt = None
        self.icu_cnts = []

    def close(self, disch_time):
        self.disch_time = disch_time 

    def count(self, event, pid):
        if is_icu_in(event):
            self.nb_icu += 1
            self.now_icu_cnt = ICUCnt(event.time, self.nb_event)
            self.icu_cnts.append(self.now_icu_cnt)
        elif is_icu_leave(event):
            self.now_icu_cnt.close(event.time)
            self.now_icu_cnt = None
        elif self.now_icu_cnt is not None:
            self.now_icu_cnt.count(event)
        self.nb_event += 1

    def json_obj(self):
        obj = {
            "admit_time": str(self.admit_time),
            "disch_time": str(self.disch_time),
            "nb_icu": self.nb_icu,
            "nb_event": self.nb_event,
            "is_emergency": self.is_emergency,
            "icu_cnts":[]
        }
        for icu_cnt in self.icu_cnts:
            obj['icu_cnts'].append(icu_cnt.json_obj())
        return obj

    @staticmethod
    def load_from_json(obj):
        admit_time = parse_time(obj['admit_time'])
        disch_time = parse_time(obj['disch_time'])
        assert admit_time 
        assert disch_time
        nb_icu = obj['nb_icu']
        nb_event = obj['nb_event']
        is_emergency = obj['is_emergency']
        hos_cnt = HospitalCnt(is_emergency, admit_time)
        hos_cnt.nb_icu = nb_icu
        hos_cnt.nb_event = nb_event
        hos_cnt.disch_time = disch_time
        hos_cnt.icu_cnts = []
        for icu_cnt in obj['icu_cnts']:
            hos_cnt.icu_cnts.append(ICUCnt.load_from_json(icu_cnt))
        return hos_cnt
    
    def write(self, writer):
        writer.write("\t" + json.dumps(self.json_obj()) + "\n")

    def stat(self):
        self.duration_days = (self.disch_time - self.admit_time).days+1
        last_time = self.admit_time
        last_index = 0
        idx = 0
        for icu_cnt in self.icu_cnts:
            icu_cnt.hours_to_last = (icu_cnt.st - last_time).total_seconds()/3600
            icu_cnt.nb_events_to_last = icu_cnt.index - last_index                
            last_index = icu_cnt.index + icu_cnt.nb_event
            last_time = icu_cnt.ed
            icu_cnt.stat()
            idx += 1
        if len(self.icu_cnts) > 0:
            self.nb_events_aft_last_icu = max(self.nb_event - last_index, 0)
            self.nb_hours_aft_last_icu = max((self.disch_time - last_time).total_seconds()/3600, 0)
        else:
            self.nb_events_aft_last_icu = self.nb_event
            self.nb_hours_aft_last_icu = (self.disch_time - self.admit_time).total_seconds()/3600


class ICUCnt:
    def __init__(self, st_time, index):
        self.st = st_time
        self.ed = None
        self.index = index
        self.nb_event = 0

    def count(self, event):
        self.nb_event += 1

    def close(self, ed_time):
        self.ed = ed_time
        
    def stat(self):
        self.duration_hours = (self.ed - self.st).total_seconds() / 3600 

    def json_obj(self):
        return {
            "st": str(self.st),
            "ed": str(self.ed),
            "index": self.index,
            "nb_event": self.nb_event,
        }

    @staticmethod
    def load_from_json(obj):
        st = parse_time(obj['st'])
        ed = parse_time(obj['ed'])
        assert st
        assert ed
        index = obj['index']
        nb_event = obj['nb_event']
        icu_cnt = ICUCnt(st, index)
        icu_cnt.ed = ed
        icu_cnt.nb_event = nb_event
        return icu_cnt

    def __cmp__(self, other):
        return cmp(self.st, other.ed)
        
def process(line):
    parts = line.rstrip().split("|")
    if len(parts) < 2:
        return None
    pid = int(parts[0])
    patient_cnt = PatientCnt(pid)
    for idx, part in enumerate(parts):
        if idx == 0:
            continue
        event = SimpleEvent.load_from_str(part)
        patient_cnt.count(event)
    return patient_cnt


def stat_event(filepath):
    writer = file(os.path.join(stat_dir, "event_seq_stat.result"), 'w')
    for line in file(filepath):
        patient_cnt = process(line)
        
        if patient_cnt is not None:
            patient_cnt.write(writer)
    writer.close()
        

if __name__ == "__main__":
    event_seq_filepath = os.path.join(result_dir, "event_seq.dat")
    stat_event(event_seq_filepath)
