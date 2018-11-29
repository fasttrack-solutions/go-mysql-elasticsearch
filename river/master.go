package river

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/fasttrack-solutions/go-mysql/mysql"
	"github.com/go-redis/redis"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
)

const (
	// redisStorageMore means that master info is stored in Redis.
	redisStorageMore string = "redis"
	// fsStorageMore means that master info is stored in filesystem.
	fsStorageMore string = "fs"

	redisMIHashKey    string = "go-mysql-elasticsearch-master-info"
	redisBinNameField string = "bin_name"
	redisBinPosField  string = "bin_pos"
)

type masterInfoData struct {
	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`
}

type masterInfo struct {
	redisClient     *redis.Client
	redisKeyPostfix string
	mode            string

	sync.RWMutex

	Data masterInfoData

	filePath     string
	lastSaveTime time.Time
}

func newMasterInfo(c *Config) (*masterInfo, error) {
	mi := new(masterInfo)

	mi.mode = c.DataStorage
	mi.redisKeyPostfix = c.RedisKeyPostfix

	switch c.DataStorage {
	case redisStorageMore:
		mi.redisClient = redis.NewClient(&redis.Options{
			Addr:     c.RedisAddr,
			Password: c.RedisPassword,
			DB:       int(c.RedisDB),
		})

		_, pingErr := mi.redisClient.Ping().Result()
		if pingErr != nil {
			return nil, pingErr
		}

		key := fmt.Sprintf("%s:%s", redisMIHashKey, mi.redisKeyPostfix)

		e := mi.redisClient.Exists(key)
		if e.Err() != nil {
			return nil, e.Err()
		}

		if e.Val() == 0 {
			mi.redisClient.HMSet(key, map[string]interface{}{
				redisBinNameField: "",
				redisBinPosField:  0,
			})
		}

	case fsStorageMore:
		if len(c.DataDir) > 0 {
			mi.filePath = path.Join(c.DataDir, "master.info")
		}

		if mkdirErr := os.MkdirAll(c.DataDir, 0755); mkdirErr != nil {
			return nil, errors.Trace(mkdirErr)
		}

	default:
		return nil, fmt.Errorf("Invalid data storage value received [%s], accepted: %s, %s", c.DataStorage, redisStorageMore, fsStorageMore)
	}

	return mi, nil
}

func (m *masterInfo) load() error {
	m.lastSaveTime = time.Now()

	switch m.mode {
	case redisStorageMore:
		key := fmt.Sprintf("%s:%s", redisMIHashKey, m.redisKeyPostfix)

		res := m.redisClient.HGetAll(key)
		if res.Err() != nil {
			return errors.Trace(res.Err())
		}

		data, dataErr := res.Result()
		if dataErr != nil {
			return errors.Trace(dataErr)
		}

		var ok bool
		m.Data.Name, ok = data[redisBinNameField]
		if !ok {
			return errors.New("Failed to read master info bin name from Redis")
		}

		var pos string
		pos, ok = data[redisBinPosField]
		if !ok {
			return errors.New("Failed to read master info bin position from Redis")
		}

		posInt, posIntErr := strconv.ParseInt(pos, 10, 64)
		if posIntErr != nil {
			return errors.Trace(posIntErr)
		}

		m.Data.Pos = uint32(posInt)

	case fsStorageMore:
		f, err := os.Open(m.filePath)
		if err != nil && !os.IsNotExist(errors.Cause(err)) {
			return errors.Trace(err)
		} else if os.IsNotExist(errors.Cause(err)) {
			return nil
		}
		defer f.Close()

		_, err = toml.DecodeReader(f, &m.Data)
		return errors.Trace(err)

	}

	return nil
}

func (m *masterInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Data.Name = pos.Name
	m.Data.Pos = pos.Pos

	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}

	switch m.mode {
	case redisStorageMore:
		key := fmt.Sprintf("%s:%s", redisMIHashKey, m.redisKeyPostfix)

		m.redisClient.HMSet(key, map[string]interface{}{
			redisBinNameField: m.Data.Name,
			redisBinPosField:  m.Data.Pos,
		})

	case fsStorageMore:
		if len(m.filePath) == 0 {
			return nil
		}

		m.lastSaveTime = n
		var buf bytes.Buffer
		e := toml.NewEncoder(&buf)

		e.Encode(m.Data)

		var err error
		if err = ioutil2.WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
			log.Errorf("canal save master info to file %s err %v", m.filePath, err)
			return errors.Trace(err)
		}
	}

	return nil
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Data.Name,
		Pos:  m.Data.Pos,
	}
}

func (m *masterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}
