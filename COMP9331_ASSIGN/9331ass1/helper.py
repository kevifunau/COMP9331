import struct
import sys


class STP_segment_parser(object):
    '''
    input a bytes-like stream
    transfer and output
    '''
    def __init__(self,STP_segment):
        self.segment = STP_segment.decode("utf-8")
        self.MSS = 0
        self.seqNum =0
        self.ackNum = 0
        self.SYN =0
        self.ACK = 0
        self.FIN =0
        self.data = " "

    def parser(self):
        divide_segment = self.segment.split('|')
        self.seqNum = int(divide_segment[0])
        self.ackNum = int(divide_segment[1])
        self.MSS = int(divide_segment[2])
        self.ACK = int(divide_segment[3])
        self.SYN = int(divide_segment[4])
        self.FIN = int(divide_segment[5])
        self.data = divide_segment[6]



def generate_STPsegment(header , data):
    STP_segment = header + "|" + data
    return STP_segment.encode("UTF-8")

'''
sequence number -- seqNum
ackoowledgement number -- ackNum
MSS  -- tell the receiver  the size of STP segment
FLAGS: ACK SYN FIN
'''
def generate_STPheader(seqNUm = 0, ackNum= 0 ,MSS=0, ACK=0,SYN =0 , FIN =0):
    return ('|'.join([str(seqNUm),str(ackNum),str(MSS),str(ACK),str(SYN),str(FIN)]))



"""
read file from current file
divide into segments
"""
def divide_file_into_segment(filename,client_isn,MSS):
    try:
        with open(filename,"rb") as f:

            file = f.read().decode("utf-8")
            filesize=len(file)
            if filesize == 0:
                segmentDict = {client_isn+1:""}
            else:
                segmentDict = {client_isn + 1 + i: file[i:i + MSS] for i in
                               range(0, len(file), MSS)}
            return segmentDict,filesize
    except FileNotFoundError:
        print("File not found")
        return


if __name__ =='__main__':
    segment_dict = divide_file_into_segment("test1.txt",100,20)
    for i in segment_dict.items():
        print (i)

    # header = generate_STPheader(1,2,2,1,1,1)
    # data = segment_dict[21]
    # STP_segment1 = generate_STPsegment(header, "")
    # print (STP_segment1)
    # seg = STP_segment_parser(STP_segment1)
    # seg.parser()
    # print (seg.data,seg.ackNum)
    #
    #
    # s = header.encode()
    # print (header.encode())
    # print (sys.getsizeof(s))
    # headerSegment = struct.calcsize("iii????")
    # print (headerSegment)
















