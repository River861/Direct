ans = 0

with open("./macro_throughput.txt", "r") as f:
    for row in f.readlines():
        _, tp = row.strip().split(":")
        ans += int(tp)
print("sum = " + str(ans))
