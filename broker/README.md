# Broker

The broker does all of the heavy lifting with comunicating with clients and services outside of the applications. All comunication is done via a custom protocol.

## Comunication Protocol

The comunication protocol is a layer that sits on top of a TCP connection.
For now, there are only two types of requests that the broker accepts, consumer or producer requests.

### Consumer and Producer Overview

Protocol flow for the consumer and producer clients are as follows:

1. Client opens up tcp connection
1. Client sends a message via the socket in the format of `{consumer/producer}:{topic}:{offset}` offset is only used for consumers as this is where the read takes place
1. Broker confirms with an AWK message
1. The consumer client is ready to recieve messages from the broker and resposnd with AWK when one is recieved or the producer client can send messages for the broker to injest and reply with AWK
