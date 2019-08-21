import re
from util import result_dir
import os

pattern = re.compile(r"\s{2,}")
outf = file(os.path.join(result_dir, 'sorted_stat.tsv'), 'w')
stats = []
for line in file(os.path.join(result_dir, 'stat.tsv')):
    if line.startswith('****') or line.strip() == "":
        continue
    else:
        nentry  = int(pattern.split(line)[1])
        stats.append((nentry, line))

stats.sort(key = lambda x:x[0], reverse = True)
for nentry, line in stats:
    outf.write(line)
outf.close()


