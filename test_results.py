from unittest import TestCase

filename = "test.log"
with open(filename) as f:
    content = f.readlines()
content = [line.strip() for line in content]

count_return_data = 0
count_return_already = 0
count_return_executed = 0
for line in content:
    if "return data" in line:
        count_return_data += 1
    elif "return already" in line:
        count_return_already += 1
    elif "return executed" in line:
        count_return_executed += 1

total = count_return_executed + count_return_already + count_return_data

print "return data {count}".format(count=count_return_data)
print "return already {count}".format(count=count_return_already)
print "return executed {count}".format(count=count_return_executed)
print "sum {sum}".format(sum=total)
