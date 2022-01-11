import pyftdc
import datetime

p = pyftdc.FTDCParser()

start = datetime.datetime.now()
p.parse_dir('/home/jorge/diagnostic.data', lazy=False)
end = datetime.datetime.now()

t = end - start

print(t)
