package broker

import (
	"bytes"
	"encoding/binary"
	"time"
)

type Message struct {
	Size      uint64
	Timestamp time.Time
	Key       []byte
	Value     []byte
}

//TODO: improve the encoding process, this is probably really inefficient
func (message *Message) Encode() ([]byte, error) {
	buffer := make([]byte, 264)
	timeThing := message.Timestamp.UnixMilli()
	//Write the time
	n := binary.PutUvarint(buffer, uint64(timeThing))
	//Write the key
	n += binary.PutUvarint(buffer[n:], uint64(len(message.Key)))
	buffer = buffer[:n]
	buffer = append(buffer, message.Key...)

	//Write the value
	buffer2 := make([]byte, 264)
	n2 := binary.PutUvarint(buffer2, uint64(len(message.Value)))
	buffer2 = buffer2[:n2]
	buffer2 = append(buffer2, message.Value...)

	buffer = append(buffer, buffer2...)

	return buffer, nil
}

func Decode(data []byte) (Message, error) {
	var mt Message
	reader := bytes.NewReader(data)
	ts, err := binary.ReadUvarint(reader)
	if err != nil {
		return mt, err
	}
	keySize, err := binary.ReadUvarint(reader)
	if err != nil {
		return mt, err
	}
	buf := make([]byte, keySize)
	reader.Read(buf)

	valueSize, err := binary.ReadUvarint(reader)
	if err != nil {
		return mt, err
	}
	buf2 := make([]byte, valueSize)
	reader.Read(buf2)

	mt = Message{
		Timestamp: time.Unix(0, int64(ts)*int64(time.Millisecond)),
		Key:       buf,
		Value:     buf2,
	}
	return mt, nil
}
