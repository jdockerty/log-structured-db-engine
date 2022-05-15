# Log Structured Database Engine

A toy implementation of a log structured database engine, inspired from [Designing Data Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/) (DDIA) book by Martin Kleppmann.


Martin shows that you can create the world's simplest database using the follow lines of `bash`

```bash
db_set() {
    echo "$1,$2" >> database
}

db_get() {
    grep "^$1," database | sed -e "s/^$1,//" | tail -n 1
}
```

This is my own not-so-concise implementation in Go, used to solidify the concepts that Martin portrays. Comments are provided for explanation and understanding.

### Example

Build using `go build db.go` and then run

```bash
./db --entry "1, foo"
./db --entry "2, bar"
./db --get "1" # outputs 'foo'
./db --entry "1, bar" # updates ID 1 to bar
./db --get "1" # outputs 'bar'
./db --disable-index --get "1" # also outputs 'bar', but with a full scan returning the latest entry
```