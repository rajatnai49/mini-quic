set -e

go build -o server ./cmd/server
go build -o client ./cmd/client
head -c 5000000 /dev/urandom > in.bin

./server &
sleep 1

./client -file in.bin

cmp in.bin out.bin && echo "PASS"
