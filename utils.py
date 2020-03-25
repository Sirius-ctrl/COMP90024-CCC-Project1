import json
import os
from collections import Counter
import string

def make_line(line):
    # remove , and \n
    # TODO: later change it more general for the origial file as the last one does end with a ']}}'
    return line[:-2]



class lessReader:
    """ read the file line by line and return a generator"""

    def __init__(self, name):
        self.target = open(name, 'r')
    
    def __iter__(self):
        return self
    
    def __next__(self):
        try:
            line = next(self.target)
            return line
        except StopIteration:
            return "EOF"

if __name__ == "__main__":
    # print(load_data()['rows'][0]['doc']['lang'])
    # tweets = load_data()
    # print(simple_cumulator(tweets))
    lr = lessReader("smallTwitter.json")
    header = next(lr)
    line = make_line(next(lr))
    # i = 1

    # while(line != "EOF"):
    #     try:
    #         json.loads(make_line(line))
    #     except:
    #         print(i)
    #         print(line)
    #         exit(0)
    #     line = next(lr)
    #     i += 1
    print(json.loads(line)['doc']['lang'])


