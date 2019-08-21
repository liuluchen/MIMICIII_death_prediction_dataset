from util import *
from build_sample_setting import PatientSample, load_death_sample_setting
from build_event import Event
import glob
import json


def init_sample(sample_setting_map, max_event_len):
    sample_map = {}
    for pid in sample_setting_map:
        sample_setting = sample_setting_map[pid]
        sample = PatientSample(sample_setting, max_event_len)
        sample_map[pid] = sample
    return sample_map

def load_event(filepath, sample_map):
    logging.info('load event from %s' % os.path.basename(filepath))
    for line in file(filepath):
        sys.stdout.flush()
        event = Event.load_from_line(line)
        pid = event.pid
        if pid in sample_map:
            sample_map[pid].add_event(event)


if __name__ == "__main__":
    max_event_len = 1000

    sample_setting_path = os.path.join(stat_dir, "death_sample_setting.txt")
    sample_setting_map = load_death_sample_setting(sample_setting_path)
    out_path = os.path.join(death_exper_dir, "death_sample_len=%d.txt" %max_event_len)
    if not os.path.exists(death_exper_dir):
        logging.info('mkdir %s' % death_exper_dir )
        os.mkdir(death_exper_dir)

    sample_map = init_sample(sample_setting_map, max_event_len)
    for filepath in glob.glob(event_dir + '/*.tsv'):
        load_event(filepath, sample_map)

    writer = file(out_path, 'w')
    for pid in sorted(sample_map.keys()):
        sample_map[pid].write(writer)
    writer.close()