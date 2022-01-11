import pyftdc
import datetime

p = pyftdc.FTDCParser()

start = datetime.datetime.now()
p.parse_file('./tests/diagnostic.data_40/metrics.2021-07-22T17-16-31Z-00000', lazy=False)
end = datetime.datetime.now()

t = end - start
print(t)
