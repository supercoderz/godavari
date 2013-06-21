from godavari.common import OnGrid,Serialize
from threading import Thread
import time

def data():
    return 1

def test():
    @OnGrid('hello','localhost',2207,data,False)
    @Serialize
    def hello(number):
        time.sleep(5)
        return number
    return hello

class Runner(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        print test()

for i in range(50):
    print i
    r=Runner()
    r.start()
