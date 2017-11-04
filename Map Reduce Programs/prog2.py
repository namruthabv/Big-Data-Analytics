import collections
import os
import sys

os.system('python map_reduce_prog2.py -r emr sample_white_house.csv > out_file.txt')
freq_number = sys.argv[1]

dic = {}
with open("out_file.txt") as f:
    for line in f:
       (key, val) = line.split("\t")
       dic[key] = int(val)

print(collections.Counter(dic).most_common(int(freq_number)))
