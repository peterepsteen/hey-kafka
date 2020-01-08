package app

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"

	"gopkg.in/go-playground/validator.v9"

	"github.com/peterepsteen/hey-kafka/pkg/avroproducer"
	"github.com/peterepsteen/hey-kafka/pkg/schemareg"
)

// Config configures the application with needed parameters
type Config struct {
	Schema                string `validate:"required"`
	Message               string `validate:"required"`
	Host                  string `validate:"required"`
	Port                  int    `validate:"required"`
	Topic                 string `validate:"required"`
	SchemaRegistryAddress string `mapstructure:"schema-registry-address"`
}

var validate = validator.New()

var successTemplateString = `
Success sending message.
	Topic: %s
	Offset: %d
`

// Run is the entrypoint into the application. It will do everything needed
// to send the avro message
func Run(conf *Config) (err error) {
	if err := validate.Struct(conf); err != nil {
		return err
	}

	producer := getProducer(conf)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case s := <-producer.Successes():
			log.Printf(successTemplateString, s.Topic, s.Offset)
		case err = <-producer.Errors():
		}
	}()

	schema := getSchema(conf)
	message := getMessage(conf)

	producer.Produce(conf.Topic, schema, message)
	wg.Wait()
	return
}

func getSchema(conf *Config) []byte {
	return readFileOrReturnRaw(conf.Schema)
}

func getMessage(conf *Config) []byte {
	return readFileOrReturnRaw(conf.Message)
}

func readFileOrReturnRaw(pathOrInput string) []byte {
	if _, err := os.Stat(pathOrInput); err != nil {
		// file doesnt exist, assume Schema is a string schema
		return []byte(pathOrInput)
	}

	b, err := ioutil.ReadFile(pathOrInput)
	if err != nil {
		log.Fatalf("Error reading schema as file: %s", err.Error())
	}

	return b
}

func getProducer(conf *Config) *avroproducer.AvroProducer {
	opts := []avroproducer.Opt{}
	if conf.SchemaRegistryAddress != "" {
		reg, err := schemareg.New(conf.SchemaRegistryAddress, &http.Client{})
		if err != nil {
			log.Fatalf("Error getting schema registry: %s", err.Error())
		}
		opts = append(opts, avroproducer.WithSchemaRegistry(reg))
	}

	p, err := avroproducer.New(fmt.Sprintf("%s:%d", conf.Host, conf.Port), opts...)
	if err != nil {
		log.Fatalf("Error getting avro producer: %s", err.Error())
	}
	return p
}
