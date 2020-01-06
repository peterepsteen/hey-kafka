package avroproducer

import (
	"encoding/binary"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
)

// ISchemaRegistry is a client interacting with a schema registry
type ISchemaRegistry interface {
	RegisterSchema(codec *goavro.Codec, subject string) (id int, err error)
}

// AvroProducer sends avro encoded messages to kafka,
// with an optional use of a schema registry to register avro
// schemas
type AvroProducer struct {
	kafkaProducer  sarama.AsyncProducer
	errors         chan error
	schemaRegistry ISchemaRegistry
}

// Opt configures an AvroProducer
type Opt func(*AvroProducer) error

// WithSchemaRegistry configures the AvroProducer to use a schema registry
var WithSchemaRegistry = func(registry ISchemaRegistry) Opt {
	return func(a *AvroProducer) error {
		a.schemaRegistry = registry
		return nil
	}
}

// New instantiates and returns an AvroProducer
func New(kafkaAddress string, opts ...Opt) (*AvroProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer([]string{kafkaAddress}, config)
	if err != nil {
		return nil, err
	}

	errors := make(chan error, 1)
	avroProducer := &AvroProducer{producer, errors, nil}

	go func() {
		for saramaError := range producer.Errors() {
			errors <- saramaError
		}
	}()

	for _, opt := range opts {
		if err := opt(avroProducer); err != nil {
			return nil, err
		}
	}

	return avroProducer, nil
}

// Produce sends the avro message to kafka, and registers the schema
// with the schema registry if configured
func (p *AvroProducer) Produce(topic string, schema, message []byte) {
	codec, err := goavro.NewCodec(string(schema))
	if err != nil {
		p.errors <- fmt.Errorf(`
could not marshal schema. Error: %s
	Schema: %s`, err.Error(), string(message))
		return
	}

	native, _, err := codec.NativeFromTextual(message)
	if err != nil {
		p.errors <- fmt.Errorf(`
could not marshal message. Error: %s
	Message: %s`, err.Error(), string(message))
		return
	}

	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		p.errors <- err
		return
	}

	schemaID := 0
	if p.schemaRegistry != nil {
		schemaID, err = p.schemaRegistry.RegisterSchema(codec, topic)
		if err != nil {
			p.errors <- err
			return
		}
	}

	binaryMsg := &AvroEncoder{
		SchemaID: schemaID,
		Content:  binary,
	}

	p.kafkaProducer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: binaryMsg,
	}

}

// Errors returns a channel receiving any errors from the producer. Note that
// this channel is buffered, and thus will block if no process is receiving the errors
func (p *AvroProducer) Errors() <-chan error {
	return p.errors
}

// Successes returns a channel receiving any successes from the producer
func (p *AvroProducer) Successes() <-chan *sarama.ProducerMessage {
	return p.kafkaProducer.Successes()
}

// AvroEncoder encodes schemaId and Avro message.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// Encode encodes a message according to the confluent spec
func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte
	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by Schema Registry
	binarySchemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaID, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaID...)
	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaId and Content.
func (a *AvroEncoder) Length() int {
	return 5 + len(a.Content)
}
