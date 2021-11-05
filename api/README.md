# Nolan API

For more in depth documentation about how the clients and broker comunicate please checkout of the broker README.

## Usage

There are 2 types of clients that can be create via this api consumers and producers.

### Producer Examples

Super simple producer example

``` go
func ProduceMessage() {
    // Decided on what topic to conect to, this topic must already exist
    producer, _ := api.NewProducer("topic1") //error handle how you see fit
    message0 := []byte("hello") // Any* bytes can be sent,
    producer.ProduceMessage(message) //error handle how you see fit
    producer.ProduceMessage([]byte("world")) //error handle how you see fit
}
```

producer example using message encoding provided by nolan for advanced functionality

``` go
func ProduceMessage() {
    // Decided on what topic to conect to, this topic must already exist
    producer, _ := api.NewProducer("topic1") //error handle how you see fit
    key := []byte(fmt.Sprintf("Key %d", i))
    value := []byte(fmt.Sprintf("Value %d", i))

    message := broker.Message{
        Timestamp: time.Now(),
        Key:       key,
        Value:     value,
    }
    producer.ProduceMessage(message) //error handle how you see fit
}
```

### Consumer Examples

consumers are a little more involved

``` go
func ConsumeMessages() {
    consumer, err := api.NewConsumer("topic1", 0)
    for {
        msg, err := consumer.Consume()
        if err != nil {
            if err.Error() == "no new messages" { //This is a special message, catch it to continue polling so we dont exit prematurly
                continue
            } else { //Other errors are unexpected, we should leave this loop
                logger.Warning.Println(err) 
                break
            }
        }
        logger.Info.Printf("%v, %s, %s\n", msg.Timestamp, msg.Key, msg.Value) //Decode your message accordingly
    }
}
```
