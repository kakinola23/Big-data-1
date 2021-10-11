from mrjob.job import MRJob
from mrjob.step import MRStep

class AVG_R2(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_R2,
                   reducer=self.redR2_AVG)
        ]

    def map_R2(self, _, line):
        (id,time,R1,R2,R3,R4,R5,R6,R7,R8,Temp,Humidity) = line.split()
        if R2 not in ['R2']: 
           yield (_, float(R2)) 

    def redR2_AVG(self, _, value):
        R2 = 0
        count1 = 0
        for val in value:
            R2 = R2 + val
            count1= count1 + 1
       
        yield ('R2_Average ', R2/count1)
       

if __name__ == '__main__':
    AVG_R2.run()