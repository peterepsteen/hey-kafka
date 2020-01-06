package schemareg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"

	goavro "github.com/linkedin/goavro/v2"
)

// SchemaRegistry is a client for a confluent avro schema registry
type SchemaRegistry struct {
	host       string
	httpClient *http.Client

	subjectIDCache     map[string]int //json codec -> subject id
	subjectIDCacheLock sync.RWMutex
}

// New creates a new SchemaRegistry
func New(host string, client *http.Client) (*SchemaRegistry, error) {
	return &SchemaRegistry{host, client, map[string]int{}, sync.RWMutex{}}, nil
}

const (
	schemaByID       = "/schemas/ids/%d"
	subjects         = "/subjects"
	subjectVersions  = "/subjects/%s/versions"
	deleteSubject    = "/subjects/%s"
	subjectByVersion = "/subjects/%s/versions/%s"
	contentType      = "application/vnd.schemaregistry.v1+json"
)

type schemaResponse struct {
	Schema string `json:"schema"`
}

// RegisterSchema either gets a schema id if it already exists, or creates
// one, and returns the id
func (client *SchemaRegistry) RegisterSchema(codec *goavro.Codec, subject string) (id int, err error) {
	schemaJSON := codec.Schema()

	client.subjectIDCacheLock.RLock()
	cached, found := client.subjectIDCache[schemaJSON]
	client.subjectIDCacheLock.RUnlock()
	if found {
		return cached, nil
	}

	schema := schemaResponse{schemaJSON}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}

	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if err != nil {
		return 0, err
	}
	id, err = parseID(resp)
	if err != nil {
		return
	}

	client.subjectIDCacheLock.Lock()
	client.subjectIDCache[schemaJSON] = id
	defer client.subjectIDCacheLock.Unlock()
	return
}

func (client *SchemaRegistry) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	url := fmt.Sprintf("%s%s", client.host, uri)
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := client.httpClient.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	if !okStatus(resp) {
		return nil, fmt.Errorf("Error response from schema registry for endpoint %s: %d", uri, resp.StatusCode)
	}

	return ioutil.ReadAll(resp.Body)
}

func okStatus(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

type idResponse struct {
	ID int `json:"id"`
}

func parseID(str []byte) (int, error) {
	var id = new(idResponse)
	err := json.Unmarshal(str, &id)
	return id.ID, err
}
