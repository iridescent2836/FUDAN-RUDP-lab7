import sys
import getopt
import threading

import Checksum
import BasicSender

'''
This is a skeleton sender class. Create a fantastic transport protocol here.
'''


class Sender(BasicSender.BasicSender):
    def __init__(self, dest, port, filename, debug=False, sackMode=False):
        super(Sender, self).__init__(dest, port, filename, debug)
        self.sackMode = sackMode
        self.windowSize = 5
        self.base = 0
        self.nextSeqNum = 1
        self.sackMode = sackMode
        self.bufferSize = 1460
        self.timer = [None, None, None, None, None]
        self.TimeoutInterval = 0.5
        # store the packet inside the window
        self.data = [None, None, None, None, None]
        self.lock = threading.Lock()


    def send_start_message(self):
        start_packet = self.make_packet('start', 0, '')
        self.send(start_packet)
        self.data[0] = start_packet
        if self.sackMode:
            self.timer[0] = threading.Timer(self.TimeoutInterval, self.handle_timeout_sack, [0])
        else:
            self.timer[0] = threading.Timer(self.TimeoutInterval, self.handle_timeout)
        self.timer[0].start()
        self.log("send start packet: %s" % start_packet)
        
    def receive_ack(self):
        sack = self.receive().decode()
        if sack is not None:
            msg_type, seqno, data, checksum = self.split_packet(sack)
            if not Checksum.validate_checksum(sack):
                self.log("received corrupt ack: %s" % sack)
                
                    
            if msg_type == 'ack':
                self.log("receive ack: %s" % sack)
                self.handle_new_ack(seqno)
            elif msg_type == 'sack':
                self.log("receive sack: %s" % sack)
                self.handle_new_ack(seqno, ack_type='sack')
            return True
        
        return False
    
    
    def send_data(self):
        msg_type = 'data'
        msg = self.infile.read(self.bufferSize)
        # print(msg)
        while msg_type != 'end':
            # Send packets in the window and after base
            while self.nextSeqNum < self.base + self.windowSize and msg_type != 'end':
                next_msg = self.infile.read(self.bufferSize)
                msg_type = 'data'
                # If the length of the read message is less than bufferSize,
                # it means we've finished reading, set msg_type to 'end'
                if len(next_msg) < self.bufferSize:
                    msg_type = 'end'
                packet = self.make_packet('data', self.nextSeqNum, msg)
                self.data[self.nextSeqNum % self.windowSize] = packet
                self.send(packet)
                self.log("sent: %s" % packet)
                # update timer
                if self.sackMode:
                    self.timer[self.nextSeqNum % self.windowSize] = threading.Timer(self.TimeoutInterval,
                                                                                self.handle_timeout_sack,
                                                                                [self.nextSeqNum])
                else:
                    self.timer[self.nextSeqNum % self.windowSize] = threading.Timer(self.TimeoutInterval,
                                                                            self.handle_timeout)
                self.timer[self.nextSeqNum % self.windowSize].start()
                
                self.nextSeqNum += 1
                msg = next_msg
                
            self.receive_ack()

            
        return msg
    
    def send_end_message(self, msg):
        end_packet = self.make_packet('end', self.nextSeqNum, msg)
        self.send(end_packet)
        self.data[self.nextSeqNum % self.windowSize] = end_packet
        if self.sackMode:
            self.timer[self.nextSeqNum % self.windowSize] = threading.Timer(self.TimeoutInterval,
                                                                        self.handle_timeout_sack,
                                                                        [self.nextSeqNum])
        else:
            self.timer[self.nextSeqNum % self.windowSize] = threading.Timer(self.TimeoutInterval,
                                                                            self.handle_timeout)
            
        self.timer[self.nextSeqNum % self.windowSize].start()
        self.log("sent: %s" % end_packet)
        self.nextSeqNum = self.nextSeqNum + 1
    
    # Main sending loop.
    def start(self):
        if self.sackMode:
            self.log("sack")
        else:  # go back n
            self.log("go back n")
            
        self.send_start_message()
        if self.receive_ack():
            msg = self.send_data()
            self.send_end_message(msg) 

        # Receive the remaining acks
        while self.base < self.nextSeqNum:
            self.receive_ack()
        self.log("done sending")

    def handle_timeout(self):
        # Reset the timer and resend packets from base to nextSeqNum
        for i in range(self.base, self.nextSeqNum):
            self.timer[i % self.windowSize].cancel()
            self.timer[i % self.windowSize] = threading.Timer(self.TimeoutInterval, self.handle_timeout)
            self.timer[i % self.windowSize].start()
            packet = self.data[i % self.windowSize]
            self.send(packet)
            # print('timeout resend: %s' % packet)
            self.log("resend: %s" % packet)

    def handle_timeout_sack(self, num):
        # Only resend the timed-out packet
        self.timer[num % self.windowSize].cancel()
        # print('timeout resend',num)
        self.timer[num % self.windowSize] = threading.Timer(self.TimeoutInterval, self.handle_timeout_sack, [num])
        self.timer[num % self.windowSize].start()
        packet = self.data[num % self.windowSize]
        self.send(packet)
        self.log("timeout resend: %s" % packet)

    def handle_new_ack(self, ack, ack_type='ack'):
        if self.sackMode:  # Selective Acknowledgements
            # print(ack)
            if ack_type == 'sack':
                # sack|<cum_ack;sack1,sack2,sack3,...>|<checksum>
                cum_ack, sack = ack.split(';')
                # strip
                cum_ack = cum_ack.strip()
                sack = sack.strip()
                sack = sack.split(',')
                # Remove non-numeric elements from sack
                sack = [int(i) for i in sack if i.isdigit()]
                
                if int(cum_ack) > self.base:
                    for i in range(self.base,int(cum_ack)):
                        if self.timer[i % self.windowSize] is not None:
                            self.timer[i % self.windowSize].cancel()
                        self.timer[self.base % self.windowSize] = None
                    # print(self.timer)
                    # print('self base',self.base)
                    # print('cum_ack',cum_ack)
                    self.base = int(cum_ack)
                    self.log("received ack: %s" % cum_ack)
                elif int(cum_ack) == self.base:
                    self.handle_dup_ack(int(cum_ack))
                    
                for i in sack:
                    # If the timer for the corresponding packet in sack is
                    # still running, remove it from timer
                    print('not in sequence',i)
                    if self.timer[i % self.windowSize] is not None:
                        self.timer[i % self.windowSize].cancel()
                        self.timer[i % self.windowSize] = None
                        self.log("received sack: %s" % i)
            else:
                if ack == self.base:
                    # print(self.base, self.nextSeqNum)
                    self.handle_dup_ack(ack)
                elif int(ack) > self.base:
                    # print('ack', ack, self.nextSeqNum)
                    self.timer[self.base % self.windowSize].cancel()
                    self.timer[self.base % self.windowSize] = None
                    # print(self.timer)
                    self.base = int(ack)
                    self.log("received ack: %s" % ack)
        else:  # go back n
            # 1.如果接收端收到了一个sequence number不为N的数据包，它会发送“ack|N”
            # 2.如果接收端收到了一个sequence number为N的数据包，它会检查
            # 自己已按序收到的数据包中序号最大的数据包，假设该数据包的sequence number为M，
            # 那么接收端会发送“ack|M+1”
            if self.base < int(ack) <= self.nextSeqNum:
                # update base
                self.base = int(ack)
                #  update timer
                for i in range(self.base, self.nextSeqNum):
                    self.timer[i % self.windowSize].cancel()
                self.log("receive ack: %s" % ack)

    def handle_dup_ack(self, ack):
         # If a duplicate ack is received, it means
         # the base packet has not been received.
        # Reset the timer and resend the base packet
        print('dup ack', ack)
        packet = self.data[ack % self.windowSize]
        self.send(packet)
        self.timer[ack % self.windowSize].cancel()
        self.timer[ack % self.windowSize] = threading.Timer(self.TimeoutInterval, self.handle_timeout_sack,
                                                                  [ack])
        self.timer[ack % self.windowSize].start()

    def log(self, msg):
        if self.debug:
            print(msg)


'''
This will be run if you run this script from the command line. You should not
change any of this; the grader may rely on the behavior here to test your
submission.
'''
if __name__ == "__main__":
    def usage():
        print("RUDP Sender")
        print("-f FILE | --file=FILE The file to transfer; if empty reads from STDIN")
        print("-p PORT | --port=PORT The destination port, defaults to 33122")
        print("-a ADDRESS | --address=ADDRESS The receiver address or hostname, defaults to localhost")
        print("-d | --debug Print debug messages")
        print("-h | --help Print this usage message")
        print("-k | --sack Enable selective acknowledgement mode")


    try:
        opts, args = getopt.getopt(sys.argv[1:],
                                   "f:p:a:dk", ["file=", "port=", "address=", "debug=", "sack="])
    except:
        usage()
        exit()

    port = 33122
    dest = "localhost"
    filename = None
    debug = False
    sackMode = False

    for o, a in opts:
        if o in ("-f", "--file="):
            filename = a
        elif o in ("-p", "--port="):
            port = int(a)
        elif o in ("-a", "--address="):
            dest = a
        elif o in ("-d", "--debug="):
            debug = True
        elif o in ("-k", "--sack="):
            sackMode = True

    s = Sender(dest, port, filename, debug, sackMode)
    try:
        s.start()
    except (KeyboardInterrupt, SystemExit):
        exit()