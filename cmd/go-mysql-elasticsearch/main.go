package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/fasttrack-solutions/go-mysql-elasticsearch/river"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	myc "github.com/siddontang/go-mysql/client"
)

var configFile = flag.String("config", "./etc/river.toml", "go-mysql-elasticsearch config file")

var data_storage = flag.String("data_storage", "", "Data storage")
var mappings_dir = flag.String("mappings_dir", "", "Mappings directory")

var my_addr = flag.String("my_addr", "", "MySQL addr")
var my_user = flag.String("my_user", "", "MySQL user")
var my_pass = flag.String("my_pass", "", "MySQL password")

var redis_addr = flag.String("redis_addr", "", "Redis addr")
var redis_pass = flag.String("redis_pass", "", "Redis password")
var redis_db = flag.Int("redis_db", -1, "Redis database")

var es_addr = flag.String("es_addr", "", "Elasticsearch addr")
var data_dir = flag.String("data_dir", "", "path for go-mysql-elasticsearch to save data")
var server_id = flag.Int("server_id", 0, "MySQL server id, as a pseudo slave")
var flavor = flag.String("flavor", "", "flavor: mysql or mariadb")
var execution = flag.String("exec", "", "mysqldump execution path")
var logLevel = flag.String("log_level", "info", "log level")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	log.SetLevelByName(*logLevel)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	if len(*my_addr) > 0 {
		cfg.MyAddr = *my_addr
	}

	if len(*my_user) > 0 {
		cfg.MyUser = *my_user
	}

	if len(*my_pass) > 0 {
		cfg.MyPassword = *my_pass
	}

	if *server_id > 0 {
		cfg.ServerID = uint32(*server_id)
	}

	if len(*es_addr) > 0 {
		cfg.ESAddr = *es_addr
	}

	if len(*data_dir) > 0 {
		cfg.DataDir = *data_dir
	}

	if len(*flavor) > 0 {
		cfg.Flavor = *flavor
	}

	if len(*execution) > 0 {
		cfg.DumpExec = *execution
	}

	if *redis_db > -1 {
		cfg.RedisDB = uint32(*redis_db)
	}

	if len(*redis_addr) > 0 {
		cfg.RedisAddr = *redis_addr
	}

	if len(*redis_pass) > 0 {
		cfg.RedisPassword = *redis_pass
	}

	if len(*mappings_dir) > 0 {
		cfg.MappingsDir = *mappings_dir
	}

	// Reconnect to MySQL.
	for {
		_, err = myc.Connect(cfg.MyAddr, cfg.MyUser, cfg.MyPassword, "")
		if err != nil {
			log.Printf("Failed to connect to MySQL [%s], reconnecting in 10 seconds\n", cfg.MyAddr)
			time.Sleep(10 * time.Second)
			continue
		}

		break
	}

	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	done := make(chan struct{}, 1)
	go func() {
		r.Run()
		done <- struct{}{}
	}()

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	}

	r.Close()
	<-done
}
