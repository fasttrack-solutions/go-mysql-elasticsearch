package verificator

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/ashwanthkumar/slack-go-webhook"
	"github.com/fasttrack-solutions/go-mysql-elasticsearch/river"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/robfig/cron"
)

// NoTTL simple enum to avoid re-typing
const NoTTL = -1

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
	SuicideCountRedisKey          string
	ServiceIsAllowedToRunRedisKey string

	brandID int

	myUser string
	myPass string
	myAddr string

	slackWebhookURL  string
	slackChannelName string
}

// InitAndStart initializes verificator and cron
func InitAndStart(r *river.River, verificatorCronSpec string, brandID int, slackWebhookURL, slackChannelName, redisAddr, redisPassword,
	redisKeyPostfixSuicideCount, redisKeyPostfixAllowedToRun string,
	redisDB int, myUser, myPass, myAddr string, secondsThreshold, unSyncedThreshold int, errorChan chan (error)) error {
	fmt.Println("Starting verificator")
	var err error

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})

	v := Verificator{
		overThresholdCounter:          0,
		zeroDate:                      time.Now(),
		threshold:                     uint32(unSyncedThreshold),
		secondsThreshold:              secondsThreshold,
		redisClient:                   redisClient,
		SuicideCountRedisKey:          "go-mysql-elasticsearch-suicide-count",
		ServiceIsAllowedToRunRedisKey: "go-mysql-elasticsearch-allowed-to-run",
		brandID:                       brandID,
		myUser:                        myUser,
		myPass:                        myPass,
		myAddr:                        myAddr,
		slackWebhookURL:               slackWebhookURL,
		slackChannelName:              slackChannelName,
	}

	if redisKeyPostfixSuicideCount != "" {
		v.SuicideCountRedisKey = v.SuicideCountRedisKey + redisKeyPostfixSuicideCount
	}

	if redisKeyPostfixAllowedToRun != "" {
		v.ServiceIsAllowedToRunRedisKey = v.ServiceIsAllowedToRunRedisKey + redisKeyPostfixAllowedToRun
	}

	err = v.doVerificationCheck(r)
	if err != nil {
		return err
	}
	c := cron.New()
	c.AddFunc(verificatorCronSpec, func() {
		err = v.doVerificationCheck(r)
		if err != nil {
			errorChan <- err
		}
	})
	c.Start()

	return nil
}

// MySQLMasterStatus contains resulset from MYSQL
type MySQLMasterStatus struct {
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
		fmt.Println(err)
	}

	v.suicideCount, err = v.currentSuicideCount()
	if err != nil {
		return err
	}

	v.currentBinLogDiff = positionsDiff(mysqlMasterStatus.Position, r.PublicPosition().Pos)
	if v.currentBinLogDiff > v.threshold {
		v.overThresholdCounter++
	} else if v.currentBinLogDiff == 0 {
		v.zeroDate = time.Now()
		v.overThresholdCounter = 0
	}

	if r.PublicPosition().Pos == 0 {
		fmt.Println("redis position 0, probably doing mysqldump")
		return nil
	}

	if v.overThresholdCounter == 0 {
		v.resetSuicideCount()
	} else if v.overThresholdCounter > 0 {
		secondsSinceOverdraw := v.secondsUnsynced()

		if secondsSinceOverdraw > float64(v.secondsThreshold) && v.suicideCount >= 2 {
			v.setServiceAsDisallowedToRun()
			err = v.sendSlackWarning("🔥 Shutting down shipper. Setting not allowed to run = true.")
			if err != nil {
				return err
			}
			v.commitSuicide()
		} else if secondsSinceOverdraw > float64(v.secondsThreshold) && v.suicideCount >= 1 {
			err = v.sendSlackWarning("⚠️ Restarting shipper go mysql due to suicide count > 1.")
			if err != nil {
				return err
			}
			v.commitSuicide()
		} else if secondsSinceOverdraw > float64(v.secondsThreshold) {
			err = v.sendSlackWarning("️️⚠️ Restarting shipper go mysql.")
			if err != nil {
				return err
			}
			v.commitSuicide()
		}
	}

	return nil
}

func (v *Verificator) getMySQLMasterStatus() (MySQLMasterStatus, error) {
	var status MySQLMasterStatus

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
	v.redisClient.Incr(v.SuicideCountRedisKey)
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
	res, err := v.redisClient.Get(v.SuicideCountRedisKey).Result()
	if err == redis.Nil {
		err := v.setDefaultRedisKey(v.SuicideCountRedisKey, "0")
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
	_, err := v.redisClient.Set(v.SuicideCountRedisKey, 0, NoTTL).Result()
	if err != nil {
		return err
	}
	return nil
}

func (v *Verificator) serviceIsAllowedToRun() (bool, error) {
	res, err := v.redisClient.Get(v.ServiceIsAllowedToRunRedisKey).Result()
	if err == redis.Nil {
		err := v.setDefaultRedisKey(v.ServiceIsAllowedToRunRedisKey, "1")
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
	_, err := v.redisClient.Set(v.ServiceIsAllowedToRunRedisKey, "0", NoTTL).Result()
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

func (v *Verificator) sendSlackWarning(message string) error {
	if v.slackWebhookURL == "" || v.slackChannelName == "" {
		return errors.New("No slack url or channel name set")
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

	err := slack.Send(v.slackWebhookURL, "", payload)
	if len(err) > 0 {
		return err[0]
	}

	return nil
}
