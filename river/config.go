package river

import (
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// SourceConfig is the configs for source
type SourceConfig struct {
	Schema string   `toml:"schema"`
	Tables []string `toml:"tables"`
}

// Config is the configuration
type Config struct {
	TT TimeTracker

	DataStorage string
	MappingsDir string

	MyAddr     string
	MyUser     string
	MyPassword string
	MyCharset  string

	RedisAddr       string
	RedisPassword   string
	RedisDB         uint32
	RedisKeyPostfix string

	ESHttps    bool
	ESAddr     string
	ESUser     string
	ESPassword string

	StatAddr string

	ServerID uint32
	Flavor   string
	DataDir  string

	DumpExec       string
	SkipMasterData bool

	Sources []SourceConfig `toml:"source"`

	Rules []*Rule `toml:"rule"`

	BulkSize int

	FlushBulkTime time.Duration

	SkipNoPkTable bool
}

// NewConfigWithFile creates a Config from file.
func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

// NewConfig creates a Config from data.
func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
