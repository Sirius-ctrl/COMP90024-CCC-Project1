import json
from collections import Counter
import string
import subprocess
import re

def process_line(line):
    hashtags = re.findall("#[a-zA-Z0-9_]+", line['doc']['text'].lower())
    
    return [line['doc']['lang']], hashtags


def make_line(line):
    line = line.strip()
    if line[-1] == ",":
        return line[:-1]
    
    return line

def illustrate(counter, name):
    print("====================  " + name + "  ====================")
    
    # ranking from 1
    i = 1
    for k,v in counter:

        if i <= 9:
            print(str(i)+". ", k+",", str(v))
        else:
            print(str(i)+".", k+",", str(v))
        i += 1

    print("======================" + "="*len(name) + "======================\n")

class lessReader:
    """ read the file line by line and return a generator"""

    def __init__(self, name):
        self.target = open(name, 'r')
    
    def __iter__(self):
        return self
    
    def __next__(self):
        try:
            line = next(self.target)
            if len(line) > 10:
                return line
            else:
                return "EOF"
        except StopIteration:
            return "EOF"

if __name__ == "__main__":
    import subprocess
    out = subprocess.Popen(["wc", "-l", "smallTwitter.json"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = out.communicate()
    print(int(stdout.split()[0]))

