from os import listdir
from os.path import isfile, join

mypath = "/tmp/resultWiki/"
dirs = [ join(mypath,dirs) for dirs in listdir(mypath) if not isfile(join(mypath,dirs)) ]
files = [ join(d,f) for d in dirs for f in listdir(d) ]

new_file = mypath + "results"
wf = open(new_file, "w")
for f in files:
    of = open(f, "r")
    for line in of:
        wf.write(line)
    of.close()
wf.close()
