build:
	cd main; go build -o ../opentick main.go; cd -

test:
	go test

benchmark:
	go test -test.bench='.*'
