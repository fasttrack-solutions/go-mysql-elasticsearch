package elastic

import (
	"flag"
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

var host = flag.String("host", "127.0.0.1", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")

func Test(t *testing.T) {
	TestingT(t)
}

type elasticTestSuite struct {
	c *Client
}

var _ = Suite(&elasticTestSuite{})

func (s *elasticTestSuite) SetUpSuite(c *C) {
	cfg := new(ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	s.c, _ = NewClient(cfg)
}

func (s *elasticTestSuite) TearDownSuite(c *C) {

}

func makeTestData(arg1 string, arg2 string) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = arg1
	m["content"] = arg2

	return m
}

func (s *elasticTestSuite) TestSimple(c *C) {
	index := "test-simple"

	// Delete defore running the test.
	s.c.DeleteIndex(index)

	err := s.c.Update(index, "1", makeTestData("abc", "hello world"))
	c.Assert(err, IsNil)

	exists, err := s.c.Exists(index)
	c.Assert(err, IsNil)
	c.Assert(exists, Equals, true)

	r, err := s.c.Get(index, "1")
	c.Assert(err, IsNil)
	c.Assert(r.Code, Equals, 200)
	c.Assert(r.ID, Equals, "1")

	err = s.c.Delete(index, "1")
	c.Assert(err, IsNil)

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		items[i] = req
	}

	resp, err := s.c.IndexBulk(index, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Code, Equals, 200)
	c.Assert(resp.Errors, Equals, false)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.ID = id
		items[i] = req
	}

	resp, err = s.c.IndexBulk(index, items)
	c.Assert(err, IsNil)
	c.Assert(resp.Code, Equals, 200)
	c.Assert(resp.Errors, Equals, false)
}
