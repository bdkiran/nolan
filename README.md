# Nolan: Lightwieght, fast, and resiliant event streaming

Nolan is a tool that allows for messaging infrastrucure. It provides fast but resiliant messaging to build stream and Pub-Sub messaging.

## Getting Started

Since the app is written in Go, it needs to be compiled and run.

For basic running and debuging

```bash
$ go run .
$
```

To run a production build

```bash
$ go install ./...
$
```

```bash
$ main
$
```

## Design

Nolan utilizes a commit log to provide message resiliancy. This type of starage is used by a variety of existing products such as Kafka, Postgresql and others to provide fast and strong garuntees about message retention.

## Roadmap

1. Implement a simple API for consuming an producing messages.
1. Extensive unit testing to provide significant coverage.
1. Establish benchmarks to provide context about the speed and reliabilty.
1. Wiki about the interals of how Nolan works.
