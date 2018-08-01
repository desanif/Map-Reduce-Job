from mrjob.job import MRJob
import urllib
from time import sleep
import operator
import time

global di
di = {}
global counter
counter = 0

class NodeCount(MRJob):
    
    def map(self, _, line):
        """
        Output every user in the file
        """
        #print("Length is " + str(len(line)))
        t = line.split(',')
        #print(t[1]+ " " +t[0])
        '''
        for i in range(20):
            print("str[" + str(i) + "] is " + str(t[i]))
        '''
        key = str(t[22])
        '''
        global counter
        counter += 1
        if t[19] == '/' or t[19] == "/":
            print(counter)
            print(line)
        #print("t[19] is " + str(t[19]))
        
        if "POTUS" in key:
            key = "POTUS"
            yield key, 1
        elif not ("VISITORS" in key) and not "NA" in key:
        '''
        if not "NA" in key:
            yield key, 1

    
    def combiner(self, key, value):
        
        """
        Sum all the occurences of every user
        This will let us see how many unique users are there by reducing
        """
        yield key, sum(value)
    
    def reduce(self, key, value):
        #sorted(key, reverse=True)
        #print("In reduce")
        """
        Counts all unique users from combiner data
        We ignore value, which is just how many times each user has appeared in the file
        """
        global di
        for i in value:
                #print(key, i)
                di[key] = i
        #sorted_x = sorted(d.items(), key=operator.itemgetter(1))
    
        #z=sorted(value, key=int, reverse=True)
        #print(z)

    def steps(self):
        """
        the steps can be modified to compose any number of map/reduce steps
        by including multiple instances of self.mr
        """
        return [self.mr(mapper=self.map, combiner = self.combiner, reducer=self.reduce)]

if __name__ == '__main__':
    NodeCount.run()
    i = 0
    z = sorted(di.items(), key = operator.itemgetter(1), reverse=True)
    for x in z:
        if i >= 10:
            break
        print(x)
        i += 1
    #print(str(time.time() - start))
