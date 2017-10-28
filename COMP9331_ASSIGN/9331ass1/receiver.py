import socket
import helper
import random
import sys
import time
from collections import defaultdict

###### receiver section ########
class STP_receiver(object):
    # the receiver should accept two parameters
    # receiver_port:
    # file--filename : file being transmitted from sender
    def __init__(self,receiver_port,filename):
        self.receiver_port = receiver_port
        self.filename = filename
        self.host = ''
        self.STATE = "CLOSED"
        self.MSS= 0
        self.expectedPacket=0
        self.firstReceivePactetSeq=0


        # file.txt and log.txt
        self.file =0
        self.receiverLog=0

        self.AmountData=0
        self.NumDataReceived=0

        self.totalDuplicate =0
        self.segmentCount = 0
    def ackpacketSeq(self):
        lisst = sorted(list(self.databuffer.keys()))
        if len(lisst) ==1:
            return lisst[0]
        for i in range(1, len(lisst)):
            if lisst[i] - lisst[i - 1] != self.MSS:
                return lisst[i - 1]
            if lisst[i] == lisst[-1]:
                return lisst[-1]
    def _receive(self):
        STP_segment, addr = self.socket.recvfrom(1024)
        _STP_segment = helper.STP_segment_parser(STP_segment)
        _STP_segment.parser()
        return _STP_segment,addr
    def initial_socket(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.host, self.receiver_port))
        self.server_isn = random.randint(0,2**16)
        self.STATE = "LISTEN"

        print ("server is waiting for UDP connection")
        # log information
        self.receiverLog = open("Receiver_log.txt", 'w')
        self.receiverLog = open("Receiver_log.txt", 'a')
    def process(self):
        while True:
            if self.STATE == "LISTEN":
                _stp_segment,addr = self._receive()
                self.start_time = time.time()
                # SYN segment arrived
                if _stp_segment.SYN == 1 and _stp_segment.ACK ==0:
                    self.receiverLog.write("{:6}{:<6.3f}\tS\t{}\t0\t{}\n".format("rcv",(time.time() - self.start_time)*1000,_stp_segment.seqNum,_stp_segment.ackNum))
                    # allocate STP buffer and variable to the connection
                    self.databuffer =defaultdict(int)
                    #self.dataReceived=[]
                    #self.seqlist=[]
                    self.MSS = _stp_segment.MSS
                    #connection-granted segment(SYN ACK)
                    header = helper.generate_STPheader(self.server_isn,_stp_segment.seqNum+1,self.MSS,1,1,0)
                    response = helper.generate_STPsegment(header,"")
                    self.socket.sendto(response,addr)
                    self.receiverLog.write("{:6}{:<6.3f}\tSA\t{}\t0\t{}\n".format("snd",(time.time() - self.start_time)*1000,self.server_isn,_stp_segment.seqNum+1))
                    self.STATE ="SYN_RCVD"
            elif self.STATE == "SYN_RCVD":
                _stp_segment,addr = self._receive()
                if _stp_segment.ACK == 1 and _stp_segment.SYN == 0:

                    self.receiverLog.write("{:6}{:<6.3f}\tA\t{}\t0\t{}\n".format("rcv",(time.time() - self.start_time)*1000,_stp_segment.seqNum,_stp_segment.ackNum))
                    self.firstReceivePactetSeq = _stp_segment.seqNum
                    self.STATE= "EST"

                    # create a new file called file.txt
                    self.file = open(self.filename,'w')
                    self.file = open(self.filename,'a')
            elif self.STATE =="EST":
                _stp_segment,addr = self._receive()
                if _stp_segment.FIN != 1:
                    ####### trans section #########
                    self.segmentCount+=1
                    self.receiverLog.write("{:6}{:<6.3f}\tD\t{}\t{}\t{}\n".format("rcv",(time.time() - self.start_time)*1000,_stp_segment.seqNum,len(_stp_segment.data),_stp_segment.ackNum))
                    #receive packet
                    self.databuffer[_stp_segment.seqNum] = _stp_segment.data
                    if self.databuffer[self.firstReceivePactetSeq] == 0 :
                        self.expectedPacket = self.firstReceivePactetSeq
                    else:
                        # using ackpacketseq function to return the maximum order packet
                        if len(_stp_segment.data) ==0 :
                            self.expectedPacket = self.ackpacketSeq() +1
                        else:
                          self.expectedPacket = self.ackpacketSeq() + len(_stp_segment.data)
                    #send ack
                    header = helper.generate_STPheader(self.server_isn + 1, self.expectedPacket, self.MSS, 1, 0, 0)
                    response = helper.generate_STPsegment(header, "")
                    self.socket.sendto(response, addr)
                    self.receiverLog.write("{:6}{:<6.3f}\tA\t{}\t0\t{}\n".format("snd",(time.time() - self.start_time)*1000,_stp_segment.ackNum,self.expectedPacket))

                else:

                    # finish trans work , restore the data into file
                    filereceive = [ (x,y) for x,y in self.databuffer.items()]
                    self.NumDataReceived = len(filereceive)
                    filereceive.sort()
                    for x,y in filereceive:
                        self.file.write(y)
                    self.file.close()

                    with open(self.filename,'rb') as f:
                        self.AmountData = len(f.read())

                    self.totalDuplicate = self.segmentCount - self.NumDataReceived



                    ######### FIN SECTION ############
                    self.receiverLog.write("{:6}{:<6.3f}\tF\t{}\t0\t{}\n".format("rcv",(time.time() - self.start_time)*1000,_stp_segment.seqNum,_stp_segment.ackNum))
                    # ready to close
                    header = helper.generate_STPheader(_stp_segment.ackNum,_stp_segment.seqNum+1,self.MSS,1,0,0)
                    response = helper.generate_STPsegment(header,"")
                    self.socket.sendto(response,addr)
                    self.receiverLog.write("{:6}{:<6.3f}\tFA\t{}\t0\t{}\n".format("snd",(time.time() - self.start_time)*1000,_stp_segment.ackNum,_stp_segment.seqNum+1))
                    self.STATE = "CLOSE_WAIT"
                    if self.STATE == "CLOSE_WAIT":
                        header = helper.generate_STPheader(_stp_segment.ackNum, _stp_segment.seqNum+1, self.MSS, 0, 0, 1)
                        response = helper.generate_STPsegment(header,"")
                        self.socket.sendto(response, addr)
                        self.STATE = "LAST_ACK"
            elif self.STATE=="LAST_ACK":
                _stp_segment,addr = self._receive()
                if _stp_segment.ACK ==1:
                    self.STATE ="WAIT"
                    self.receiverLog.write("{:6}{:<6.3f}\tA\t{}\t0\t{}\n".format("rcv",(time.time() - self.start_time)*1000,_stp_segment.seqNum,_stp_segment.ackNum))
                    time.sleep(1)
                    self.socket.close()

                    self.STATE = "CLOSED"

                    self.receiverLog.write("Amount of (orginial) Data Recivied (in bytes) %d\n"% self.AmountData )
                    self.receiverLog.write("Number of (original) Data Segments Received %d\n"% self.NumDataReceived)
                    self.receiverLog.write("Number of duplicate segments received %d\n"% self.totalDuplicate)

                    self.receiverLog.close()
                    print("server over")
                    break

#### main ####



receiver_port = int(sys.argv[1])
filename = sys.argv[2]
receiver = STP_receiver(receiver_port,filename)
receiver.initial_socket()
receiver.process()


