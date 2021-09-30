# Commitlog

## Design

The log is the heart of this data storage system as it provides a reliable source of truth. The Commitlog in particular prioritizes fast append-only writes and reads. It is made up of 3 main structures/objects: commitlog, segment, and index. The goal is to keep the source code of the commitlog under 500 lines, not including tests.

### Segment

The segment represents where are actual data is being stored. Data is written to a log file in monotomic order.

### Index

The index provides a record for where every message is kept. Every log file has a corresponding index file. This allows for fast and out of order reads of the messages within the log file.

## Usage

The using the commitlog is easy. To enforce secure usage and simplicity only a few functions are exported and operations from the outside are only done through the commitlogl

``` go
func main() {
    // Create a new comitlog 'partition'
    cl, _ := commitlog.New("logs/partition")
    cl.Append([]byte("Hello World"))
    cl.Append([]byte("Welcome back"))
    cl.Append([]byte("Last one"))
    
    // "pop" the lastest entry
    cl.ReadLatestEntry()
    // "Randomly" access and read a message
    cl.Read(0)
}

```

## To Implement

- Multiple segments implementation
- Cleaner. Need to be able to clean up old logs? How do we delineate older logs?
- Reader? Not sure if we want to wrap a io.Reader interface
