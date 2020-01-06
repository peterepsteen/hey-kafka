package schemareg

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/linkedin/goavro/v2"

	"github.com/spf13/viper"
)

//TODO:fix tests

var schemaRegistry *SchemaRegistry

func init() {
	replacer := strings.NewReplacer("-", "_", ".", "_")
	viper.SetEnvKeyReplacer(replacer)

	viper.SetDefault("schema-host", "http://localhost:8081")

	var err error
	schemaRegistry, err = New(viper.GetString("schema-host"), http.Client{})
	if err != nil {
		panic(err)
	}
}

var testSchema = `
{
    "type": "record",
    "name": "UserCreated",
    "fields": [
        {"name": "userId", "type": "long"},
        {"name": "firstReferrer", "type": "string"},
        {"name": "firstReferrerPath", "type": "string"},
        {"name": "firstReferrerDomain", "type": "string"},
        {"name": "firstReferrerCategory", "type": "string"},
        {"name": "firstReferrerSubCategory", "type": "string"},
        {"name": "ts", "type": "long"}
    ]
}
`

func TestCreateSubject(t *testing.T) {
	codec, err := goavro.NewCodec(testSchema)
	if err != nil {
		panic(err)
	}

	subject := fmt.Sprintf("test-UserCreated-%d", time.Now().Unix())
	id, err := schemaRegistry.RegisterSchema(codec, subject)
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
	log.Printf("Created schema id: %d", id)
}

func TestExistingSubject(t *testing.T) {
	codec, err := goavro.NewCodec(testSchema)
	if err != nil {
		panic(err)
	}

	subject := fmt.Sprintf("test-UserCreated-%d", time.Now().Unix())
	id, err := schemaRegistry.RegisterSchema(codec, subject)
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
	log.Printf("Created schema id: %d", id)

	id2, err := schemaRegistry.RegisterSchema(codec, subject)
	log.Printf("Second schema id: %d", id)
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
	assert.Equal(t, id, id2)
}
