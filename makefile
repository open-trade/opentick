build:
	cd main; go build -o ../opentick main.go; cd -

test:
	go test

run:
	cd main; go run main.go; cd -

benchmark:
	go test -test.bench='.*'
