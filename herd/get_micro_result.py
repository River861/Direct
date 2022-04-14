
ans = {op: 0 for op in ['READ', "UPDATE", "INSERT", "DELETE"]}

with open("./micro_throughput.txt", "r") as f:
    for row in f.readlines():
        _, op, tp = row.strip().split(":")
        ans[op] += int(tp)

for op, tp in ans.items():
    print(f"{op}: {tp}")
