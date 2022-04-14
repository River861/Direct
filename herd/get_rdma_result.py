ans = 0

with open("./rdma_throughput.txt", "r") as f:
    for row in f.readlines():
        _, tp = row.strip().split(":")
        ans += int(tp)
print("sum = " + str(ans))
