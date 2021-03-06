# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}

#
# HACK!!!
# Change this to the memcached's machine IP
#
export HRD_REGISTRY_IP="10.10.1.9"

export MLX5_SINGLE_THREADED=1

if [ "$#" -ne 1 ]; then
    blue "Illegal number of parameters"
	blue "Usage: ./run-machine.sh <machine_number>"
	exit
fi

blue "Removing hugepages"
shm-rm.sh 1>/dev/null 2>/dev/null

num_threads=8		# Threads per client machine
: ${HRD_REGISTRY_IP:?"Need to set HRD_REGISTRY_IP non-empty"}

blue "Running $num_threads client threads"

sudo rm macro_throughput.txt
sudo rm micro_throughput.txt
sudo rm rdma_throughput.txt
sudo rm lock_throughput.txt

sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	numactl --cpunodebind=0 --membind=0 ./main \
	--num-threads $num_threads \
	--base-port-index 0 \
	--num-server-ports 1 \
	--num-client-ports 1 \
	--is-client 1 \
	--update-percentage 50 \
	--machine-id $1
