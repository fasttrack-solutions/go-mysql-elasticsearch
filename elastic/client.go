package elastic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/juju/errors"
	es "gopkg.in/olivere/elastic.v6"
)

// Client is the client to communicate with ES.
// Although there are many Elasticsearch clients with Go, I still want to implement one by myself.
// Because we only need some very simple usages.
type Client struct {
	Protocol string
	Addr     string
	User     string
	Password string

	c *http.Client
}

// ClientConfig is the configuration for the client.
type ClientConfig struct {
	HTTPS       bool
	Addr        string
	User        string
	Password    string
	MappingsDir string
}

const (
	// StatusYellow represents yellow ES cluster status.
	StatusYellow string = "yellow"
	// StatusGreen represents green ES cluster status.
	StatusGreen string = "green"
)

func waitForES(url string) error {
	type esResp struct {
		Status string `json:"status"`
	}

	for {
		resp, respErr := http.Get(fmt.Sprintf("%s/_cluster/health?pretty", url))
		if respErr != nil || resp.StatusCode != http.StatusOK {
			log.Printf("Failed to connect to ES [%s], reconnecting in 5 seconds\n", url)

			time.Sleep(time.Second * 5)
			continue
		}

		b, bErr := ioutil.ReadAll(resp.Body)
		if bErr != nil {
			return bErr
		}

		resp.Body.Close()

		var respData esResp
		dataErr := json.Unmarshal(b, &respData)
		if dataErr != nil {
			return dataErr
		}

		if strings.ToLower(respData.Status) == StatusYellow || strings.ToLower(respData.Status) == StatusGreen {
			break
		}

		log.Printf("ES status is [%s], reconnecting in 5 seconds\n", respData.Status)
		time.Sleep(time.Second * 5)
	}

	return nil
}

func createIndexes(url, mappingsDir string) error {
	// List available mappings.
	files, filesErr := ioutil.ReadDir(mappingsDir)
	if filesErr != nil {
		return filesErr
	}

	idxMappings := make(map[string]string)

	for _, f := range files {
		fullFileName := f.Name()
		idxName := fullFileName[:len(fullFileName)-len(path.Ext(fullFileName))]

		// Read mappings file.
		b, bErr := ioutil.ReadFile(path.Join(mappingsDir, fullFileName))
		if bErr != nil {
			return bErr
		}

		// Populate map with index name and the corresponding mapping JSON.
		idxMappings[idxName] = string(b)
	}

	// Connect to ES.
	ctx := context.Background()

	esClient, esClientErr := es.NewClient(es.SetURL(url))
	if esClientErr != nil {
		return fmt.Errorf("Failed to connect to ES: %v", esClientErr)
	}

	esInfo, esCode, esPingErr := esClient.Ping(url).Do(ctx)
	if esPingErr != nil {
		return fmt.Errorf("Failed to ping ES: %v", esPingErr)
	}

	log.Printf("ES responded with code %d and version %s\n", esCode, esInfo.Version.Number)

	// Create indexes, report existing ones.
	for idxName, mappingsJSON := range idxMappings {
		start := time.Now()

		esIdxExists, esIdxExistsErr := esClient.IndexExists(idxName).Do(ctx)
		if esIdxExistsErr != nil {
			log.Fatal(esIdxExistsErr)
		}

		if esIdxExists {
			log.Printf("Index %s already exists, can not apply mappings (%v)\n", idxName, time.Since(start))
			continue
		}

		log.Printf("Processing index %s ...", idxName)
		createESIndex, createESIndexErr := esClient.CreateIndex(idxName).BodyString(mappingsJSON).Do(ctx)
		if createESIndexErr != nil {
			log.Fatal(createESIndexErr)
		}

		if !createESIndex.Acknowledged {
			log.Fatalf("Creating of index %s is not acknowledged\n", idxName)
		}

		log.Printf(" Success (%v)\n", time.Since(start))
	}

	return nil
}

// NewClient creates the Cient with configuration.
func NewClient(conf *ClientConfig) (*Client, error) {
	c := new(Client)

	c.Addr = conf.Addr
	c.User = conf.User
	c.Password = conf.Password

	err := waitForES("http://" + c.Addr)
	if err != nil {
		return nil, err
	}

	err = createIndexes("http://"+c.Addr, conf.MappingsDir)
	if err != nil {
		return nil, err
	}

	if conf.HTTPS {
		c.Protocol = "https"
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		c.c = &http.Client{Transport: tr}
	} else {
		c.Protocol = "http"
		c.c = &http.Client{}
	}

	return c, nil
}

// ResponseItem is the ES item in the response.
type ResponseItem struct {
	ID      string                 `json:"_id"`
	Index   string                 `json:"_index"`
	Type    string                 `json:"_type"`
	Version int                    `json:"_version"`
	Found   bool                   `json:"found"`
	Source  map[string]interface{} `json:"_source"`
}

// Response is the ES response
type Response struct {
	Code int
	ResponseItem
}

// See http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/bulk.html
const (
	ActionCreate = "create"
	ActionUpdate = "update"
	ActionDelete = "delete"
	ActionIndex  = "index"
)

// BulkRequest is used to send multi request in batch.
type BulkRequest struct {
	Action   string
	Index    string
	ID       string
	Pipeline string

	Data map[string]interface{}
}

func (r *BulkRequest) bulk(buf *bytes.Buffer) error {
	meta := make(map[string]map[string]string)
	metaData := make(map[string]string)

	if len(r.Index) > 0 {
		metaData["_index"] = r.Index
	}

	if len(r.ID) > 0 {
		metaData["_id"] = r.ID
	}

	if len(r.Pipeline) > 0 {
		metaData["pipeline"] = r.Pipeline
	}

	metaData["_type"] = "_doc"

	meta[r.Action] = metaData

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	buf.Write(data)
	buf.WriteByte('\n')

	switch r.Action {
	case ActionDelete:
		//nothing to do
	case ActionUpdate:
		doc := map[string]interface{}{
			"doc": r.Data,
		}
		data, err = json.Marshal(doc)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	default:
		//for create and index
		data, err = json.Marshal(r.Data)
		if err != nil {
			return errors.Trace(err)
		}

		buf.Write(data)
		buf.WriteByte('\n')
	}

	return nil
}

// BulkResponse is the response for the bulk request.
type BulkResponse struct {
	Code   int
	Took   int  `json:"took"`
	Errors bool `json:"errors"`

	Items []map[string]*BulkResponseItem `json:"items"`
}

// BulkResponseItem is the item in the bulk response.
type BulkResponseItem struct {
	Index   string          `json:"_index"`
	Type    string          `json:"_type"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Status  int             `json:"status"`
	Error   json.RawMessage `json:"error"`
	Found   bool            `json:"found"`
}

// MappingResponse is the response for the mapping request.
type MappingResponse struct {
	Code    int
	Mapping Mapping
}

// Mapping represents ES mapping.
type Mapping map[string]struct {
	Mappings map[string]struct {
		Properties map[string]struct {
			Type   string      `json:"type"`
			Fields interface{} `json:"fields"`
		} `json:"properties"`
	} `json:"mappings"`
}

// DoRequest sends a request with body to ES.
func (c *Client) DoRequest(method string, url string, body *bytes.Buffer) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(c.User) > 0 && len(c.Password) > 0 {
		req.SetBasicAuth(c.User, c.Password)
	}
	resp, err := c.c.Do(req)

	return resp, err
}

// Do sends the request with body to ES.
func (c *Client) Do(method string, url string, body map[string]interface{}) (*Response, error) {
	bodyData, err := json.Marshal(body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	buf := bytes.NewBuffer(bodyData)
	if body == nil {
		buf = bytes.NewBuffer(nil)
	}

	resp, err := c.DoRequest(method, url, buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(Response)
	ret.Code = resp.StatusCode

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret.ResponseItem)
	}

	return ret, errors.Trace(err)
}

// DoBulk sends the bulk request to the ES.
func (c *Client) DoBulk(url string, items []*BulkRequest) (*BulkResponse, error) {
	var buf bytes.Buffer

	for _, item := range items {
		if err := item.bulk(&buf); err != nil {
			return nil, errors.Trace(err)
		}
	}

	resp, err := c.DoRequest("POST", url, &buf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	ret := new(BulkResponse)
	ret.Code = resp.StatusCode

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, &ret)
	}

	return ret, errors.Trace(err)
}

// CreateMapping creates a ES mapping.
func (c *Client) CreateMapping(index string, mapping map[string]interface{}) error {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("HEAD", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	// if index doesn't exist, will get 404 not found, create index first
	if r.Code == http.StatusNotFound {
		_, err = c.Do("PUT", reqURL, nil)

		if err != nil {
			return errors.Trace(err)
		}
	} else if r.Code != http.StatusOK {
		return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
	}

	reqURL = fmt.Sprintf("%s://%s/%s/_mapping/_doc", c.Protocol, c.Addr, url.QueryEscape(index))

	_, err = c.Do("POST", reqURL, mapping)
	return errors.Trace(err)
}

// GetMapping gets the mapping.
func (c *Client) GetMapping(index string) (*MappingResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_mapping/_doc", c.Protocol, c.Addr, url.QueryEscape(index))
	buf := bytes.NewBuffer(nil)
	resp, err := c.DoRequest("GET", reqURL, buf)

	if err != nil {
		return nil, errors.Trace(err)
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := new(MappingResponse)
	err = json.Unmarshal(data, &ret.Mapping)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret.Code = resp.StatusCode
	return ret, errors.Trace(err)
}

// DeleteIndex deletes the index.
func (c *Client) DeleteIndex(index string) error {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr,
		url.QueryEscape(index))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Get gets the item by id.
func (c *Client) Get(index string, id string) (*Response, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	return c.Do("GET", reqURL, nil)
}

// Update creates or updates the data
func (c *Client) Update(index string, id string, data map[string]interface{}) error {
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("PUT", reqURL, data)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusCreated {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Exists checks whether indice exists or not.
func (c *Client) Exists(index string) (bool, error) {
	reqURL := fmt.Sprintf("%s://%s/%s", c.Protocol, c.Addr, url.QueryEscape(index))

	r, err := c.Do("HEAD", reqURL, nil)
	if err != nil {
		return false, err
	}

	return r.Code == http.StatusOK, nil
}

// Delete deletes the item by id.
func (c *Client) Delete(index string, id string) error {
	reqURL := fmt.Sprintf("%s://%s/%s/_doc/%s", c.Protocol, c.Addr,
		url.QueryEscape(index),
		url.QueryEscape(id))

	r, err := c.Do("DELETE", reqURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if r.Code == http.StatusOK || r.Code == http.StatusNotFound {
		return nil
	}

	return errors.Errorf("Error: %s, code: %d", http.StatusText(r.Code), r.Code)
}

// Bulk sends the bulk request.
// only support parent in 'Bulk' related apis
func (c *Client) Bulk(items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/_bulk", c.Protocol, c.Addr)

	return c.DoBulk(reqURL, items)
}

// IndexBulk sends the bulk request for index.
func (c *Client) IndexBulk(index string, items []*BulkRequest) (*BulkResponse, error) {
	reqURL := fmt.Sprintf("%s://%s/%s/_bulk", c.Protocol, c.Addr,
		url.QueryEscape(index))

	return c.DoBulk(reqURL, items)
}
