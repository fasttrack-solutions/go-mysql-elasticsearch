package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alex-ant/envs"
	"github.com/fasttrack-solutions/go-mysql-elasticsearch/api"
	"github.com/fasttrack-solutions/go-mysql-elasticsearch/river"
	ttracker "github.com/fasttrack-solutions/go-mysql-elasticsearch/timeTracker"
	"github.com/fasttrack-solutions/go-mysql-elasticsearch/verificator"
	myc "github.com/fasttrack-solutions/go-mysql/client"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

var (
	apiPort = flag.Int("api-port", 3000, "HTTP API port number")

	configFile = flag.String("config", "./etc/river.toml", "go-mysql-elasticsearch config file")

	dataStorage = flag.String("dataStorage", "redis", "Data storage (redis/fs)")
	mappingsDir = flag.String("mappingsDir", "", "Mappings directory")

	myAddr    = flag.String("myAddr", "127.0.0.1:3306", "MySQL addr")
	myUser    = flag.String("myUser", "root", "MySQL user")
	myPass    = flag.String("myPass", "root", "MySQL password")
	myCharset = flag.String("myCharset", "utf8", "MySQL DB charset")

	redisAddr       = flag.String("redisAddr", "127.0.0.1:6379", "Redis addr")
	redisPass       = flag.String("redisPass", "", "Redis password")
	redisDB         = flag.Int("redisDB", 0, "Redis database")
	redisKeyPostfix = flag.String("redisKeyPostfix", "", "Redis key postfix")

	verificatorCronSpec         = flag.String("verificatorCronSpec", "*/10 * * * * *", "Schedule to run verificator to make sure service is in sync")
	redisKeyPostfixSuicideCount = flag.String("redisKeyPostfixSuicideCount", "", "Redis key postfix for suicide count")
	redisKeyPostfixAllowedToRun = flag.String("redisKeyPostfixAllowedToRun", "", "Redis key postfix for allowed to run")
	unSyncedThreshold           = flag.Int("unsynced-threshhold", 1000, "Amount of allowed unsynced binlog bytes during n threshold seconds")
	secondsThreshold            = flag.Int("unsynced-threshhold-seconds", 30, "Amount of seconds during which to check unsynced-threshold")

	esAddr  = flag.String("esAddr", "127.0.0.1:9200", "Elasticsearch addr")
	esUser  = flag.String("esUser", "", "Elasticsearch user")
	esPass  = flag.String("esPass", "", "Elasticsearch password")
	esHTTPS = flag.Bool("esHTTPS", false, "Use HTTPS for ES")

	dataDir        = flag.String("dataDir", "./go-mysql-elasticsearch-data", "Path for go-mysql-elasticsearch to save data")
	statAddr       = flag.String("statAddr", "127.0.0.1:12800", "Inner HTTP status address")
	serverID       = flag.Int("serverID", 1001, "MySQL server ID, as a pseudo slave")
	flavor         = flag.String("flavor", "mysql", "Flavor: mysql or mariadb")
	bulkSize       = flag.Int("bulkSize", 256, "Minimal number of items to be inserted in a single bulk")
	execution      = flag.String("exec", "mysqldump", "mysqldump execution path")
	skipMasterData = flag.Bool("skipMasterData", false, "if no privilege to use mysqldump with --master-data, we must skip it")
	logLevel       = flag.String("logLevel", "Info", "log level")

	flushBulkTime = flag.Duration("flushBulkTime", time.Millisecond*200, "Force flush the pending requests if we don't have enough items >= bulkSize")
	skipNoPkTable = flag.Bool("skipNoPkTable", false, "Ignore table without primary key")

	brandID          = flag.Int("brand-id", 0, "Brand ID")
	useSingleRedisDB = flag.Bool("use-single-redis-db", false, "Use single Redis DB (0), dismiss brand ID in keys if different DBs")
	slackWebhookURL  = flag.String("slack-webhook-url", "", "Use for sending alerts to slack")
	slackChannelName = flag.String("slack-channel-name", "", "Channel to send messages in")

	bulksToTrack = flag.Int("bulks-to-track", 100, "Bulk requests to keep in time tracker")
)

func main() {
	// Parse flags.
	flag.Parse()

	// Determine and read environment variables.
	flagsErr := envs.GetAllFlags()
	if flagsErr != nil {
		log.Fatal(flagsErr)
	}

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

	cfg.DataStorage = *dataStorage
	cfg.MappingsDir = *mappingsDir
	cfg.MyAddr = *myAddr
	cfg.MyUser = *myUser
	cfg.MyPassword = *myPass
	cfg.MyCharset = *myCharset
	cfg.RedisAddr = *redisAddr
	cfg.RedisPassword = *redisPass
	cfg.RedisDB = uint32(*redisDB)
	cfg.ESAddr = *esAddr
	cfg.ESUser = *esUser
	cfg.ESPassword = *esPass
	cfg.ESHttps = *esHTTPS
	cfg.DataDir = *dataDir
	cfg.StatAddr = *statAddr
	cfg.ServerID = uint32(*serverID)
	cfg.Flavor = *flavor
	cfg.BulkSize = *bulkSize
	cfg.DumpExec = *execution
	cfg.SkipMasterData = *skipMasterData
	cfg.FlushBulkTime = *flushBulkTime
	cfg.SkipNoPkTable = *skipNoPkTable

	ttInstance := ttracker.New(*bulksToTrack)

	cfg.TT = ttInstance

	// Add brand ID to Redis key suffix if single DB mode selected and force 0 DB.
	cfg.RedisKeyPostfix = *redisKeyPostfix
	if *useSingleRedisDB {
		cfg.RedisKeyPostfix += fmt.Sprintf("_%d", *brandID)
		cfg.RedisDB = 0
	}

	// Reconnect to MySQL.
	log.Infof("Connecting to MySQL [%s]", cfg.MyAddr)
	for {
		_, err = myc.Connect(cfg.MyAddr, cfg.MyUser, cfg.MyPassword, "")
		if err != nil {
			log.Infof("Failed to connect to MySQL [%s], reconnecting in 10 seconds", cfg.MyAddr)
			time.Sleep(10 * time.Second)
			continue
		}

		break
	}

	// Initialize API HTTP server.
	apiServer := api.New(*apiPort, ttInstance)

	// Start API HTTP server.
	apiStartErr := apiServer.Start()
	if apiStartErr != nil {
		log.Fatal(apiStartErr)
	}

	// Initialize new river.
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

	verificatorErrorChan := make(chan error)
	err = verificator.InitAndStart(r, *verificatorCronSpec, *brandID, *slackWebhookURL, *slackChannelName, *redisAddr, *redisPass, *redisKeyPostfixSuicideCount,
		*redisKeyPostfixAllowedToRun, *redisDB, *myUser, *myPass, *myAddr, *secondsThreshold, *unSyncedThreshold, verificatorErrorChan)
	if err != nil {
		log.Fatal("Could not start verificator! Err: ", err)
	}

	select {
	case n := <-sc:
		log.Infof("receive signal %v, closing", n)
	case <-r.Ctx().Done():
		log.Infof("context is done with %v, closing", r.Ctx().Err())
	case err := <-verificatorErrorChan:
		fmt.Println("Verificator error: ", err)
	}

	r.Close()
	<-done
}
