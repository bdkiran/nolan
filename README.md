# Nolan: Lightwieght, fast, and resiliant event streaming

![nolan workflow](https://github.com/bdkiran/nolan/actions/workflows/go.yml/badge.svg)

Nolan is a tool that allows for messaging infrastrucure. It provides fast but resiliant messaging to build stream and pub-sub messaging.

## Getting Started

The application is written in pure golang(for now). The makefile produces an executable in the root project libarary for easy debugging.

The application expects there to be a directory called `logs` to write the logs/indexs to. If this is your first time running the application you need to creat this directory. For a production type build this directory would probably exist elsewhere but for now this assists in debugging.

```bash
$ mkdir logs
```

## Useful Commands

To build the application

```bash
$ make build
```

To run a nolan broker

```bash
$ ./nolan
```

To run the consumer client

```bash
$ ./nolan -client=consumer
```

To run the producer client

```bash
$ ./nolan -client=producer
```

To debug the commitlog, and print out all of the entries.

```bash
$ ./nolan -debug=true
```

## Design

Nolan utilizes a commit log to provide message resiliancy. This type of starage is used by a variety of existing products such as Kafka, Postgresql and others to provide fast and strong garuntees about message retention.

## Roadmap

1. ~~ Implement a simple API for consuming an producing messages. ~~
1. Extensive unit testing to provide significant coverage.
1. Establish benchmarks to provide context about the speed and reliabilty.
1. Wiki about the interals of how Nolan works.
