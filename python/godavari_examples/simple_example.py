from godavari.common import OnGrid,Serialize

def data():
    return 1

@OnGrid('hello','localhost',2207,data,False)
@Serialize
def hello(number):
    return number

print hello
