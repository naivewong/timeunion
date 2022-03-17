import os
import sys

d = sys.argv[1]

total_size = 0
for path, dirs, files in os.walk(d):
    for f in files:
        if "log" in f:
            continue
        fp = os.path.join(path, f)
        print(fp, os.path.getsize(fp))
        total_size += os.path.getsize(fp)
print("Directory size: " + str(total_size))