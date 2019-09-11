package verificator

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ashwanthkumar/slack-go-webhook"
	"github.com/fasttrack-solutions/go-mysql-elasticsearch/river"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql" // Importing mysql driver
	"github.com/jmoiron/sqlx"
	log "github.com/siddontang/go-log/log"
)

// NoTTL simple enum to avoid re-typing
const NoTTL = -1

// Config containing properties to set up verificator
type Config struct {
	River                       *river.River
	VerificatorTickerInterval   int
	BrandID                     int
	SlackWebhookURL             string
	SlackChannelName            string
	RedisAddr                   string
	RedisPassword               string
	RedisKeyPostfixSuicideCount string
	RedisKeyPostfixAllowedToRun string
	RedisDB                     int
	MyUser                      string
	MyPass                      string
	MyAddr                      string
	SecondsThreshold            int
	UnSyncedThreshold           int
	ErrorChan                   chan (error)
}

// Verificator is the main struct for the verificator
type Verificator struct {
	suicideCount         int
	overThresholdCounter uint32
	zeroDate             time.Time
	threshold            uint32
	secondsThreshold     int
	allowedToRun         bool
	currentBinLogDiff    uint32

	redisClient                   *redis.Client
	suicideCountRedisKey          string
	serviceIsAllowedToRunRedisKey string

	brandID int

	myUser string
	myPass string
	myAddr string

	slackWebhookURL  string
	slackChannelName string

	ticker     *time.Ticker
	tickerDone chan (bool)
}

// InitAndStart initializes verificator and ticker
func InitAndStart(conf Config) (*Verificator, error) {
	log.Info("Starting verificator")
	var err error

	redisClient := redis.NewClient(&redis.Options{
		Addr:     conf.RedisAddr,
		Password: conf.RedisPassword,
		DB:       conf.RedisDB,
	})

	v := Verificator{
		overThresholdCounter:          0,
		zeroDate:                      time.Now(),
		threshold:                     uint32(conf.UnSyncedThreshold),
		secondsThreshold:              conf.SecondsThreshold,
		redisClient:                   redisClient,
		suicideCountRedisKey:          "go-mysql-elasticsearch-suicide-count",
		serviceIsAllowedToRunRedisKey: "go-mysql-elasticsearch-allowed-to-run",
		brandID:                       conf.BrandID,
		myUser:                        conf.MyUser,
		myPass:                        conf.MyPass,
		myAddr:                        conf.MyAddr,
		slackWebhookURL:               conf.SlackWebhookURL,
		slackChannelName:              conf.SlackChannelName,
		ticker:                        time.NewTicker(time.Duration(conf.VerificatorTickerInterval) * time.Second),
		tickerDone:                    make(chan bool),
	}

	if conf.RedisKeyPostfixSuicideCount != "" {
		v.suicideCountRedisKey = v.suicideCountRedisKey + conf.RedisKeyPostfixSuicideCount
	}

	if conf.RedisKeyPostfixAllowedToRun != "" {
		v.serviceIsAllowedToRunRedisKey = v.serviceIsAllowedToRunRedisKey + conf.RedisKeyPostfixAllowedToRun
	}

	err = v.doVerificationCheck(conf.River)
	if err != nil {
		return nil, err
	}

	go v.doTicker(func() {
		err = v.doVerificationCheck(conf.River)
		if err != nil {
			conf.ErrorChan <- err
			return
		}
	})

	return &v, nil
}

// Shutdown shuts down the verificator
func (v *Verificator) Shutdown() {
	log.Print("Shutting down verificator...")
	v.redisClient.Close()
	v.tickerDone <- true
}

func (v *Verificator) doTicker(callback func()) {
	for {
		select {
		case <-v.tickerDone:
			return
		case <-v.ticker.C:
			callback()
		}
	}
}

type mysqlMasterStatus struct {
	Name            string `db:"File"`
	Position        uint32 `db:"Position"`
	BinlogDoDB      string `db:"Binlog_Do_DB"`
	BinlogIgnoreDB  string `db:"Binlog_Ignore_DB"`
	ExecutedGTIDSet string `db:"Executed_Gtid_Set"`
}

func (v *Verificator) doVerificationCheck(r *river.River) error {
	var err error

	v.allowedToRun, err = v.serviceIsAllowedToRun()
	if err != nil {
		return err
	}
	if !v.allowedToRun {
		return errors.New("Service is not allowed to run")
	}

	mysqlMasterStatus, err := v.getMySQLMasterStatus()
	if err != nil {
		return err
	}

	v.suicideCount, err = v.currentSuicideCount()
	if err != nil {
		return err
	}

	v.currentBinLogDiff = positionsDiff(mysqlMasterStatus.Position, r.GetPosition().Pos)
	if v.currentBinLogDiff > v.threshold {
		v.overThresholdCounter++
	} else if v.currentBinLogDiff == 0 {
		v.zeroDate = time.Now()
		v.overThresholdCounter = 0
	}

	if r.GetPosition().Pos == 0 {
		log.Info("redis position 0, probably doing mysqldump")
		return nil
	}

	if v.overThresholdCounter == 0 {
		v.resetSuicideCount()
	} else if v.overThresholdCounter > 0 {
		secondsSinceOverdraw := v.secondsUnsynced()

		if secondsSinceOverdraw > float64(v.secondsThreshold) && v.suicideCount >= 2 {
			v.setServiceAsDisallowedToRun()
			v.sendSlackWarning("🔥 Shutting down shipper. Setting not allowed to run = true.")
			v.commitSuicide()
		} else if secondsSinceOverdraw > float64(v.secondsThreshold) && v.suicideCount >= 1 {
			v.sendSlackWarning("⚠️ Restarting shipper go mysql due to suicide count > 1.")
			v.commitSuicide()
		} else if secondsSinceOverdraw > float64(v.secondsThreshold) {
			v.sendSlackWarning("️️⚠️ Restarting shipper go mysql.")
			v.commitSuicide()
		}
	}

	return nil
}

func (v *Verificator) getMySQLMasterStatus() (mysqlMasterStatus, error) {
	var status mysqlMasterStatus

	connStr := fmt.Sprintf("%s:%s@tcp(%s)/?parseTime=true", v.myUser, v.myPass, v.myAddr)
	client, err := sqlx.Connect("mysql", connStr)
	if err != nil {
		return status, err
	}
	defer client.Close()

	err = client.Get(&status, "SHOW MASTER STATUS;")
	if err != nil {
		return status, err
	}

	return status, nil
}

func (v *Verificator) commitSuicide() {
	v.redisClient.Incr(v.suicideCountRedisKey)
	log.Fatal("terminating service")
}

func positionsDiff(mysqlPos, redisPos uint32) uint32 {
	return mysqlPos - redisPos
}

func (v *Verificator) secondsUnsynced() float64 {
	diff := time.Now().Sub(v.zeroDate).Seconds()
	return diff
}

func (v *Verificator) currentSuicideCount() (int, error) {
	res, err := v.redisClient.Get(v.suicideCountRedisKey).Result()
	if err == redis.Nil {
		err := v.setDefaultRedisKey(v.suicideCountRedisKey, "0")
		if err != nil {
			return 0, err
		}
		return 0, nil
	} else if err != nil {
		return 0, nil
	}

	count, err := strconv.Atoi(strings.TrimSpace(res))
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (v *Verificator) resetSuicideCount() error {
	_, err := v.redisClient.Set(v.suicideCountRedisKey, 0, NoTTL).Result()
	if err != nil {
		return err
	}
	return nil
}

func (v *Verificator) serviceIsAllowedToRun() (bool, error) {
	res, err := v.redisClient.Get(v.serviceIsAllowedToRunRedisKey).Result()
	if err == redis.Nil {
		err := v.setDefaultRedisKey(v.serviceIsAllowedToRunRedisKey, "1")
		if err != nil {
			return false, err
		}
		return true, nil
	} else if err != nil {
		return false, err
	}

	valBool, err := strconv.ParseBool(res)
	return valBool, nil
}

func (v *Verificator) setServiceAsDisallowedToRun() error {
	_, err := v.redisClient.Set(v.serviceIsAllowedToRunRedisKey, "0", NoTTL).Result()
	if err != nil {
		return err
	}
	return nil
}

func (v *Verificator) setDefaultRedisKey(key string, defaultValue interface{}) error {
	_, err := v.redisClient.Set(key, defaultValue, NoTTL).Result()
	if err != nil {
		return err
	}
	return nil
}

func (v *Verificator) sendSlackWarning(message string) {
	if v.slackWebhookURL == "" || v.slackChannelName == "" {
		log.Warn("Slack webhook url or slack channel name not set. Will not send slack.")
		return
	}

	attachment1 := slack.Attachment{}
	attachment1.AddField(slack.Field{
		Title: "Brand ID",
		Value: fmt.Sprintf("%d", v.brandID),
	})

	attachment1.AddField(slack.Field{
		Title: "Bin log diff threshold",
		Value: fmt.Sprintf("%d bytes", v.threshold),
	})

	attachment1.AddField(slack.Field{
		Title: "Current bin log diff",
		Value: fmt.Sprintf("%d bytes", v.currentBinLogDiff),
	})

	attachment1.AddField(slack.Field{
		Title: "Seconds unsynced",
		Value: fmt.Sprintf("%f", v.secondsUnsynced()),
	})

	payload := slack.Payload{
		Text:        message,
		Username:    "Go MySQL Elasticsearch",
		Channel:     v.slackChannelName,
		IconEmoji:   ":robot_face:",
		Attachments: []slack.Attachment{attachment1},
	}

	errors := slack.Send(v.slackWebhookURL, "", payload)
	if errors != nil {
		for _, err := range errors {
			log.Warnf("Failed to send slack message: %s. Error: %v", message, err)
		}
	}
}
