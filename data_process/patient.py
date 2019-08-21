import sys
import os
from util import *
from gather_static_data import SingleAdmission
import glob
import datetime


def get_admit_eid(admit_type):
    global admit, admit_text
    return admit[admit_text.index(admit_type)]

def build_icu_leave_event(time, delta):
    global icu_leave
    leave_time = time + datetime.timedelta(hours = delta)
    return SimpleEvent(icu_leave[0], leave_time)

class PatientEvent:
    def __init__(self, pid):
        self.pid = pid
        self.hospital_events = []
        self.valid = True

    def is_valid(self):
        return self.valid

    def add_hospital(self, admission):
        global disch
        admit_eid = get_admit_eid(admission.admit_type)
        admit_time = admission.admit_time
        admit_event = SimpleEvent(admit_eid, admit_time)

        disch_eid = disch[0]
        disch_time = admission.disch_time
        disch_event = SimpleEvent(disch_eid, disch_time)

        hospital_event = HospitalEvent(admission.pid, admit_event, disch_event)
        self.hospital_events.append(hospital_event)

    def closeup_hospital(self):
        self.hospital_events.sort()
        self.check_hospital()

    def check_hospital(self):
        for i in range(len(self.hospital_events) - 1):
            hi = self.hospital_events[i]
            hj = self.hospital_events[i + 1]
            if hi.disch_event.time > hj.admit_event.time:
                self.valid = False
        for i in range(len(self.hospital_events)):
            hi = self.hospital_events[i]
            if hi.disch_event.time < hi.admit_event.time:
                self.valid = False

    def find_hospital(self, event):
        if not self.is_valid():
            return None
        for hospital in self.hospital_events:
            if hospital.contains(event):
                return hospital
        return None

    def add_event(self, event):
        if not self.is_valid():
            return False
        if is_admit(event) or is_disch(event):
            return True
        for hospital in self.hospital_events:
            if hospital.contains(event):
                hospital.add_event(event)
                return True
        return False
        
    def closeup_add_event(self):
        for hospital in self.hospital_events:
            hospital.closeup_add_event()

    def write(self, writer):
        writer.write(str(self.pid) + "|")
        first = True
        for hospital in self.hospital_events:
            hospital.write(writer, first)
            first = False
        writer.write("\n")

class HospitalEvent:
    def __init__(self, pid, admit_event, disch_event):
        self.pid = pid
        self.admit_event = admit_event
        self.disch_event = disch_event
        self.events = [self.admit_event, self.disch_event]

    def add_event(self, event, truncate = False):
        if truncate:
            event.time = max(event.time, self.admit_event.time)
            event.time = min(event.time, self.disch_event.time)
        self.events.append(event)

    def contains(self, event):
        return self.disch_event.time >= event.time and self.admit_event.time <= event.time

    def closeup_add_event(self):
        self.events.sort()

    def write(self, writer, first):
        for event in self.events:
            if not first:
                writer.write("|")
            first = False
            writer.write(str(event))

    def __cmp__(self, other):
        return cmp(self.admit_event.time, other.admit_event.time) 


class SimpleEvent:
    def __init__(self, eid, time):
        self.eid = eid
        self.time = time

    def __cmp__(self, other):
        global admit, disch
        if self.time == other.time:
            if self.eid in admit or other.eid in disch:
                return -1
            elif self.eid in disch or other.eid in admit:
                return 1
            else:
                return 0
        return cmp(self.time , other.time)

    def __str__(self):
        return "\t".join(map(str, [self.eid, self.time]))

    @staticmethod
    def load_from_str(string):
        parts = string.split('\t')
        eid = int(parts[0])
        time = parse_time(parts[1][:19])
        return SimpleEvent(eid, time)

def load_admission():
    admissions = []
    single_admission_path = os.path.join(static_data_dir, "single_admission.tsv")
    for line in file(single_admission_path):
        line = "\t".join(line.split("\t")[1:])
        admission = SingleAdmission.load_from_line(line)
        admissions.append(admission)
    return admissions

def load_admission_map():
    admission_map = {}
    single_admission_path = os.path.join(static_data_dir, "single_admission.tsv")
    for line in file(single_admission_path):
        p = line.split('\t')
        
        obj_line = "\t".join(p[1:])
        admission = SingleAdmission.load_from_line(obj_line)
        hid = int(p[0])
        admission_map[hid] = admission
    return admission_map

def init_patient(admissions):
    patient_map = {}
    for admission in admissions:
        pid = admission.pid
        if not pid in patient_map:
            patient_map[pid] = PatientEvent(pid)
        patient_map[pid].add_hospital(admission)
    for pid in patient_map:
        patient_map[pid].closeup_hospital()


    return patient_map


def load_event(filepath, patient_event_map):
    filename = os.path.basename(filepath)
    logging.info("load event from %s", filename)
    is_icu = filename == "icustays.tsv"
    for line in file(filepath):
        parts = line.strip().split('\t')
        eid = int(parts[0])
        pid = int(parts[1])
        time = parse_time(parts[3])
        if is_icu:
            duration = float(parts[2].split(":")[1])
            leave_event = build_icu_leave_event(time, duration)
            in_event = SimpleEvent(eid, time)
            patient_cnt = patient_event_map[pid]

            hos_in = patient_cnt.find_hospital(in_event)
            hos_out = patient_cnt.find_hospital(leave_event)
            if hos_in is not None or hos_out is not  None:
                hos = hos_out or hos_in
                hos.add_event(in_event, truncate = True)
                hos.add_event(leave_event, True)
            else:
                patient_cnt.valid = False
        else:
            patient_event_map[pid].add_event(SimpleEvent(eid, time))
                    

if __name__ == "__main__":
    admissions = load_admission()
    patient_event_map = init_patient(admissions)

    for filepath in glob.glob(event_dir + "/*tsv"):
        load_event(filepath, patient_event_map)
    for pid in patient_event_map:
        patient_event_map[pid].closeup_add_event()

    writer = file(os.path.join(result_dir, "event_seq.dat"), 'w')
    error_cnt = 0
    for pid in sorted(patient_event_map.keys()):
        if patient_event_map[pid].is_valid():
            patient_event_map[pid].write(writer)
        else:
            error_cnt += 1
    logging.info("error patient cnt = %d", error_cnt)
