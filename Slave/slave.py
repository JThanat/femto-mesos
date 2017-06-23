import threading


class Slave(threading.Thread):

    def __init__(self, threadID, name):
        super().__init__()

    def run(self):
        print("I'm running now")







