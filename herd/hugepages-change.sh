hugeadm --pool-pages-min 2MB:5120
mkdir -p /mnt/hugetlbfs ; mount -t hugetlbfs none /mnt/hugetlbfs
cat /sys/devices/system/node/node*/meminfo | fgrep Huge
