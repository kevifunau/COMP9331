# SIMPLE TRANSPORT PROTOCOL
import socket
import random
import helper
import threading
import sys
import time
from collections import defaultdict




#### global variable ####
# size = 0
# numPacketsSent = 0
# numDropped = 0
# numRetransmit = 0
# totalDupCount = 0



#########  Sender section  #############

class STP_sender(object):
    #### The PLD module -- emulate events of packets loss(standard) or delay(extended)
    class PLD_module(object):
        # standard version only drop have
        # 1 pdrop  (0-1)
        # 2 seed
        def __init__(self,pdrop,seed):

            global start_time
            global senderLog
            # global size
            # global numPacketsSent
            # global numDropped
            # global numRetransmit
            # global totalDupCount

            self.pdrop = pdrop
            self.seed = seed
            # create a UDP socket
            self.socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

            #sender log
            self.numDropped = 0


        # using to transmit STP segment
        def send(self,STP_data,addr):
            # parser for print
            seg = helper.STP_segment_parser(STP_data)
            seg.parser()
            # decide whether to reply or simulate packet loss
            if random.random() > self.pdrop:
                self.socket.sendto(STP_data,addr)
                senderLog.write("{:6}{:<8.3f}\tD\t{}\t{}\t{}\n".format("snd",(time.time() - start_time)*1000,seg.seqNum,len(seg.data),seg.ackNum))
            else:
                self.numDropped+=1
                senderLog.write("{:6}{:<8.3f}\tD\t{}\t{}\t{}\n".format("drop",(time.time() - start_time)*1000,seg.seqNum,len(seg.data),seg.ackNum))

        def send_withoutdrop(self,STP_segment,addr):
            self.socket.sendto(STP_segment,addr)

        def receive(self):
            STP_segment ,addr = self.socket.recvfrom(1024)
            seg = helper.STP_segment_parser(STP_segment)
            seg.parser()
            return seg,addr
    ### For the standard version
    # the sender should accept 8 arguments
    # 1 receiver_host_ip
    # 2 receiver_port
    # 3 file--filename
    # 4 maximum window size --MWS
    # 5 maximum Segments size == MSS
    # 6 timeout
    # two from PLD module
    # 7 pdrop : the probability for  STP data segment dropping.
    # 8 seed :the seed for random number generator
    def __init__(self, receiver_host_ip ,receiver_port ,filename, MWS, MSS, timeout, pdrop, seed):

        global start_time
        global senderLog
        # global size
        # global numPacketsSent
        # global numDropped
        # global numRetransmit
        # global totalDupCount

        # parameter legallty
        self.params_check( receiver_host_ip,receiver_port,filename,MWS,MSS,timeout,pdrop,seed)
        #8 arguments
        self.receiver_host_ip = receiver_host_ip
        self.receiver_port = receiver_port
        self.filename = filename
        self.MWS = MWS
        self.MSS = MSS
        self.timeout = timeout
        self.pdrop = pdrop
        self.seed = seed
        # send_file function maintain a state
        self.STATE = "CLOSED"
        # single timer
        self.timer=0
        # ISN
        self.client_isn =random.randint(0,2**16)

        # trans using Threading 1 flagA trans_sending 2 flagB trans_receive 3 flagC trans_timer
        # self.flagA = self.flagB = self.flagC = False
        self.flag = True

        # sender log
        self.size =0
        self.numPacketsSent =0
        self.numRetransmit =0
        self.totalDupCount=0
    def send_file(self):
        # seed
        random.seed(self.seed)
        # create a socket for send or receive packet
        self.PLD_socket = self.PLD_module(self.pdrop,self.seed)
        self.initialSeqnum =0
        self.initialackNum = 0
        self.finalSeqNum = 0
        while True:
            if self.STATE == "CLOSED":
                # start three-way handshake
                # start connect to receiver , send SYN segment
                header = helper.generate_STPheader(self.client_isn,0,self.MSS,0,1,0)
                data = ""
                _stp_segment = helper.generate_STPsegment(header,data)
                self.PLD_socket.send_withoutdrop(_stp_segment,(receiver_host_ip,receiver_port))

                senderLog.write("{:6}{:<8.3f}\tS\t{}\t{}\t{}\n".format("snd",(time.time() - start_time)*1000,self.client_isn,len(data),0))

                self.STATE ="SYN_SENT"
            elif self.STATE == "SYN_SENT":
                _stp_segment,addr = self.PLD_socket.receive()
                if _stp_segment.ACK == 1 and _stp_segment.SYN == 1:

                    senderLog.write("{:6}{:<8.3f}\tSA\t{}\t0\t{}\n".format("rcv", (time.time() - start_time)*1000, _stp_segment.seqNum,
                                                      _stp_segment.ackNum))

                    # 1, allocate STP buffer and variable to the connection
                    self.file_dict={}
                    # 2, send ACK
                    header = helper.generate_STPheader(self.client_isn+1,_stp_segment.seqNum+1,self.MSS,1,0,0)
                    data=""
                    response = helper.generate_STPsegment(header,data)
                    self.PLD_socket.send_withoutdrop(response,addr)

                    senderLog.write("{:6}{:<8.3f}\tA\t{}\t{}\t{}\n".format("snd",(time.time() - start_time)*1000, self.client_isn+1, len(data),
                                                      _stp_segment.seqNum + 1,))

                    self.initialSeqnum = self.client_isn+1
                    self.initialackNum = _stp_segment.seqNum +1

                    self.STATE ="EST"
            elif self.STATE == "EST":
                # divide file and restore it in dict
                self.file_dict,self.size = helper.divide_file_into_segment(self.filename, self.client_isn, self.MSS)


                # initial two arguments for transmission
                self.NextSeqNum = self.initialSeqnum
                self.SendBase = self.initialSeqnum
                # create for duplicate ACK check
                self.recv_count = defaultdict(int)

                self.STATE = "TRANS"
            elif self.STATE =="TRANS":
                self._process()
            elif self.STATE == "FIN":
                # FIN segments
                header = helper.generate_STPheader(self.finalSeqNum, self.initialackNum, self.MSS, 0, 0, 1)
                data = ""
                _stp_segment = helper.generate_STPsegment(header, data)
                self.PLD_socket.send_withoutdrop(_stp_segment, (receiver_host_ip, receiver_port))
                senderLog.write("{:6}{:<8.3f}\tF\t{}\t0\t{}\n".format("snd",(time.time() - start_time)*1000, self.finalSeqNum, self.initialackNum))
                self.STATE = "FIN_WAIT_1"
            elif self.STATE == "FIN_WAIT_1":
                _stp_segment,addr = self.PLD_socket.receive()
                if _stp_segment.ACK ==1:
                    self.STATE ="FIN_WAIT_2"
                    #print ("rcv {} A {} 0 {}\n".format(time.time(), _stp_segment.seqNum, _stp_segment.ackNum))
            elif self.STATE =="FIN_WAIT_2":
                _stp_segment,addr = self.PLD_socket.receive()
                senderLog.write("{:6}{:<8.3f}\tFA\t{}\t0\t{}\n".format("rcv",(time.time() - start_time)*1000, _stp_segment.seqNum,_stp_segment.ackNum))

                    #if _stp_segment.FIN ==1:
                header = helper.generate_STPheader(self.finalSeqNum+1,self.initialackNum+1,self.MSS,1,0,0)
                data=""
                _stp_segment = helper.generate_STPsegment(header,data)
                self.PLD_socket.send_withoutdrop(_stp_segment,addr)

                senderLog.write("{:6}{:<8.3f}\tA\t{}\t0\t{}\n".format("snd",(time.time() - start_time)*1000, self.finalSeqNum+1, self.initialackNum+1))

                self.STATE = "TIME_WAIT"
                time.sleep(0.1)


                senderLog.write("Amount of (original) Data Transferred(in bytes): %d\n" % self.size)
                senderLog.write("Number of Data Segments Sent (excluding retransmissions): %d\n" % self.numPacketsSent)
                senderLog.write("Number of (all)packets dropped:(by the PLD module) %d\n" % self.PLD_socket.numDropped)
                senderLog.write("Number of Retransmitted segments: %d\n" % self.numRetransmit)
                senderLog.write("Number of Duplicate Acknowledgements received: %d\n" % self.totalDupCount)
                self.PLD_socket.socket.close()



                break
            else:
                break
    def _process(self):
            self.lock = threading.Lock()
            self.t1= threading.Thread(target=self._trans_sendThread)
            self.t2= threading.Thread(target=self._trans_receiveThread)
            self.t3= threading.Thread(target=self._trans_timerThread)
            self.t1.start()
            self.t2.start()
            self.t3.start()
            self.t1.join()
            self.t2.join()
            self.t3.join()
    def _trans_sendThread(self):

        while self.STATE == "TRANS":
            if self.NextSeqNum - self.SendBase < self.MWS:
                if self.NextSeqNum > max([i for i in self.file_dict.keys()]):
                    continue
                # send packet
                header = helper.generate_STPheader(self.NextSeqNum,self.initialackNum,self.MWS,0,0,0)
                data = self.file_dict[self.NextSeqNum]
                _stp_segment = helper.generate_STPsegment(header,data)
                # if timer not running
                if(self.timer ==None):
                    self.timer = time.time()
                self.PLD_socket.send(_stp_segment,(receiver_host_ip,receiver_port))
                self.numPacketsSent+=1
                self.NextSeqNum += self.MSS
    def _trans_receiveThread(self):
        while self.STATE == 'TRANS':
            # receive ack
            data,addr = self.PLD_socket.receive()

            # before each receiving , lock the _trans_timerThread
            # reason: _trans_receiveThread must update Sendbase before _trans_timerThread
            self.lock.acquire()
            # >>>>>>>>>> lock
            senderLog.write("{:6}{:<8.3f}\tA\t{}\t{}\t{}\n".format("rcv",(time.time() - start_time)*1000,data.seqNum,len(data.data),data.ackNum))
            if self.recv_count[data.ackNum] == 0 and data.ackNum > self.SendBase:
                #  the receiver acknowledge: all data received
                if data.ackNum> max([i for i in self.file_dict.keys()]):
                    self.finalSeqNum = data.ackNum
                    # enter FIN section
                    self.STATE = "FIN"
                    # kill _trans_timerThread
                    self.flag = False
                    self.lock.release()
                    break
                else:
                    # update sendbase
                    self.SendBase = data.ackNum
                    self.timer = time.time()

             # <<<<<<<<<<<< unlock
            self.lock.release()


            # log text
            self.recv_count[data.ackNum] +=1
            if self.recv_count[data.ackNum] >1:
                self.totalDupCount+=1

            ########## fast retransmission #########
            if self.recv_count[data.ackNum] >= 4:
                self.recv_count[data.ackNum] = 1
                #send packet
                header = helper.generate_STPheader(self.SendBase, self.initialackNum,self.MSS,0,0,0)
                data = self.file_dict[self.SendBase]
                _stp_segment = helper.generate_STPsegment(header,data)
                self.PLD_socket.send(_stp_segment,(receiver_host_ip,receiver_port))
                self.timer = time.time()
                self.numRetransmit+=1
    def _trans_timerThread(self):

        while self.STATE =="TRANS":
            self.lock.acquire()
            # >>>>>>>>>>lock
            if self.flag:
                ##### timeout retransmission #######
                if time.time() - self.timer > self.timeout:
                    # timeout send packet
                    header = helper.generate_STPheader(self.SendBase, self.initialackNum, self.MSS, 0, 0, 0)
                    data = self.file_dict[self.SendBase]
                    _stp_segment = helper.generate_STPsegment(header, data)
                    self.PLD_socket.send(_stp_segment, (receiver_host_ip, receiver_port))
                    self.numRetransmit += 1
                    self.timer = time.time()

            # <<<<<<<<<<unlock
            self.lock.release()
















    def params_check(self, receiver_host_ip, receiver_port, filename, MWS, MSS, timeout, pdrop, seed):
        pass










### main ###

if __name__ =='__main__':

    receiver_host_ip = sys.argv[1]
    receiver_port = int(sys.argv[2])
    filename = sys.argv[3]
    MWS = int(sys.argv[4])
    MSS = int(sys.argv[5])
    timeout = float(sys.argv[6])/1000
    pdrop = float(sys.argv[7])
    seed = int(sys.argv[8])


    # receiver_host_ip = "127.0.0.1"
    # receiver_port = 12345
    # filename = "test2.txt"
    # MWS = 500
    # MSS = 100
    # timeout = 200.0 / 1000
    # pdrop = 0.1
    # seed = 300

    senderLog = open('Sender_log.txt', 'w')
    senderLog = open('Sender_log.txt', 'a')
    start_time = time.time()

    sender = STP_sender(receiver_host_ip, receiver_port, filename, MWS, MSS, timeout, pdrop, seed)
    sender.send_file()

    senderLog.close()
