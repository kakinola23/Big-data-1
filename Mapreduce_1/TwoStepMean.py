from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class AVG_R2(MRJob):

    def steps(self):
        return [
            MRStep(map_init=self.map_init,
                   map=self.map,
                   Fin_map=self.Fin_map,
                   combine=self.combine,
                   reduce=self.reduce),
           MRStep(map=None,
                  reduce=self.reduce2)
        ]

    def map_init(self):
        self.num = 0
        self.R2_col1 = []

    def map(self, _, line):
        (id,time,R1,R2,R3,R4,R5,R6,R7,R8,Temp,Humidity) = line.split()
        
        if R2 not in ['R2']: 
            self.num = self.num + 1  
            self.R2_col1.append(R2)
            
    def Fin_map(self):
        N = self.num
        count2 = 0
        
        for val in self.R2_col1:
            yield (int(count2/np.sqrt(N)), (float(val), 1))
            count2 =count2 + 1 
    
    def average(self, value):
        sum2 =0
        count2 = 0
        for val, z in value:
            sum2 = sum2 + val
            count2 = count2 + z
        return sum2/count2
    
    def sum_Col(self, value):
        sum2 = 0
        count2 = 0
        for val, z in value:
            sum2 =sum2 + val
            count2 =count2 + z
        return (sum2, count2)
            
    def combine(self, key, value):
        yield (key, self.sum_Col(value))

    def reduce(self, key, value):
        yield (key, self.sum_Col(value))
        
        
    def reduce2(self, _, value):   
        yield _, self.average(value) 
    
        
if __name__ == '__main__':
    AVG_R2.run()