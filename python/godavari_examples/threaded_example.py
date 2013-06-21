from godavari.common import OnGrid,Serialize
from threading import Thread
import time

def data():
    return 1

@OnGrid('hello','localhost',2207,data,False)
@Serialize
def hello(number):
    time.sleep(5)
    return number

class Runner(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        print hello

for i in range(10):
    r=Runner()
    r.start()
