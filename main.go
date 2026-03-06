package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"loadTest/monitor"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

/*
how to run load test:
go run main.go [NumScheduledMessages] [ModifyCountPerMessage] [ScheduleTimeIntervalMin] [ScheduledMsgGroupingCount]

so if you want to schedule 2000 messages with 10 modifications each,
with a 1 minute interval between schedule times,
and group 100 messages to have the same schedule time, you can run:
go run main.go 2000 10 1 100
*/

/*
NumScheduledMessages:
the total number of messages to be scheduled in the load test.
It will be divided by ModifyCountPerMessage to determine how many unique schedule IDs will be used in the test.
*/
var NumScheduledMessages = 2000

/*
ModifyCountPerMessage:
the number of times each message will be modified with a new scheduled time.
It will determine how many times each schedule ID will be reused in the test.
The scheduled time for each modification will be calculated based on the initial scheduled time and the InitDelayTime.
*/
var ModifyCountPerMessage = 10

/*
InitDelayTime:
the initial delay time (in minutes) for the first scheduled time of each message.
It will be used to calculate the scheduled time for each modification of the message by subtracting the ModifyCountPerMessage from it.
*/
var InitDelayTime int

/*
ScheduleTimeIntervalMin:
the time interval (in minutes) between the scheduled times of messages.
It will be multiplied by the group number to determine the scheduled time for each message.
*/
var ScheduleTimeIntervalMin = 1

/*
ScheduledMsgGroupingCount:
the number of messages that will be grouped together to have the same scheduled time.
It will be used to calculate the scheduled time for each message by dividing the index of the message by it to determine the group number.
*/
var ScheduledMsgGroupingCount = 100

const natsURL = "nats://localhost:4222"
const StreamName = "schedulerStream"
const pendingSubjectPrefix = "scheduler.pending"
const onProcessSubjectPrefix = "scheduler.process"
const monitorURL = "http://localhost:8222/varz?js=true"
const monitorLogFile = "loadResult.log"
const monitorInterval = 30 * time.Second

var natsClient NatsClient

type NatsClient struct {
	Conn               *nats.Conn
	JetStream          jetstream.JetStream
	Stream             jetstream.Stream
	Consumer           jetstream.Consumer
	ConsumeContextList []jetstream.ConsumeContext
}

var wg sync.WaitGroup
var latencyAggregatorWg sync.WaitGroup

var latencyChan chan time.Duration

var FirstScheduledAt time.Time
var LastScheduledAt time.Time

var latenciesMu sync.Mutex
var latencies []time.Duration

var totalLatency time.Duration
var maxLatency time.Duration
var messageCount int64

var publishFailCount int
var publishFailMu sync.Mutex

func main() {
	setLoadTestArguments()

	latencyChan = make(chan time.Duration, NumScheduledMessages)
	latencies = make([]time.Duration, 0, NumScheduledMessages)

	if err := SetupNatsClient(); err != nil {
		log.Fatalf("failed to setup nats client: %v", err)
	}
	defer natsClient.Conn.Close()

	monitorRunner := monitor.StartVarzMonitor(context.Background(), monitor.Config{
		URL:         monitorURL,
		LogFile:     monitorLogFile,
		Interval:    monitorInterval,
		HTTPTimeout: 5 * time.Second,
	})

	latencyAggregatorWg.Add(1)
	go latencyAggregator()

	successCount := publishScheduledMessageToSchedulerStream()
	log.Printf("--- publish complete: %d succeeded, %d failed ---", successCount, NumScheduledMessages-successCount)

	wg.Add(successCount)
	log.Println("--- all scheduled messages set to pending state and ready to consume ---")

	wg.Wait()

	log.Println("--- all msgs received. close channel ---")
	close(latencyChan)
	latencyAggregatorWg.Wait()

	log.Println("load test finished, FirstScheduledAt:", FirstScheduledAt, "LastScheduledAt:", LastScheduledAt)
	schDuration := LastScheduledAt.Sub(FirstScheduledAt)
	natsMonitorSummary := monitorRunner.Stop()

	if schDuration.Minutes() > 0 {
		log.Printf("pub/sub %f scheduled msgs per minute:", float64(successCount)/schDuration.Minutes())
	}

	if messageCount > 0 {
		avgLatency := totalLatency / time.Duration(messageCount)

		latenciesMu.Lock()
		slices.Sort(latencies)
		p95Latency := percentileFromSorted(latencies, 0.95)
		p99Latency := percentileFromSorted(latencies, 0.99)
		latenciesMu.Unlock()

		log.Println("📢 Latency Results:")
		log.Printf("   Avg    : %v", avgLatency)
		log.Printf("   P95    : %v", p95Latency)
		log.Printf("   P99    : %v", p99Latency)
		log.Printf("   Max    : %v", maxLatency)
		log.Printf("   Count  : %d", messageCount)

		log.Println("📢 NATS Resource Results:")
		log.Printf("   MEM Avg: %s", formatBytesHuman(natsMonitorSummary.AvgMem))
		log.Printf("   MEM Max: %s", formatBytesHuman(float64(natsMonitorSummary.MaxMem)))
		log.Printf("   CPU Avg: %s", formatCPUPercent(natsMonitorSummary.AvgCPU))
		log.Printf("   CPU Max: %s", formatCPUPercent(natsMonitorSummary.MaxCPU))
		log.Printf("   Samples: %d (fail: %d)", natsMonitorSummary.SampleCount, natsMonitorSummary.FailCount)
	}
}

func formatBytesHuman(bytes float64) string {
	if bytes < 0 {
		bytes = 0
	}

	const (
		kiB = 1024.0
		miB = 1024.0 * 1024.0
		giB = 1024.0 * 1024.0 * 1024.0
	)

	switch {
	case bytes >= giB:
		return fmt.Sprintf("%.2f GiB", bytes/giB)
	case bytes >= miB:
		return fmt.Sprintf("%.2f MiB", bytes/miB)
	case bytes >= kiB:
		return fmt.Sprintf("%.2f KiB", bytes/kiB)
	default:
		return fmt.Sprintf("%.0f B", bytes)
	}
}

func formatCPUPercent(v float64) string {
	if v < 0 {
		v = 0
	}

	return fmt.Sprintf("%.2f%%", v)
}

func setLoadTestArguments() error {
	args := os.Args

	if len(args) >= 5 {
		numScheduledChatMessagesInt, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		NumScheduledMessages = numScheduledChatMessagesInt

		modifyCountPerMessageInt, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		ModifyCountPerMessage = modifyCountPerMessageInt

		InitDelayTime = ModifyCountPerMessage

		scheduleTimeIntervalMinInt, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		ScheduleTimeIntervalMin = scheduleTimeIntervalMinInt

		scheduledMsgGroupingCountInt, err := strconv.Atoi(args[4])
		if err != nil {
			return err
		}
		ScheduledMsgGroupingCount = scheduledMsgGroupingCountInt
	}

	return nil
}

func SetupNatsClient() error {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return err
	}

	natsClient.Conn = nc

	js, err := jetstream.New(natsClient.Conn)
	if err != nil {
		return err
	}

	natsClient.JetStream = js

	if err := createSchedulerStream(); err != nil {
		return err
	}

	if err := createSchedulerConsumer(); err != nil {
		return err
	}

	return nil
}

func createSchedulerStream() error {
	stream, err := natsClient.JetStream.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:    StreamName,
		Storage: jetstream.FileStorage,
		Subjects: []string{
			fmt.Sprintf("%s.*", pendingSubjectPrefix),
			fmt.Sprintf("%s.*", onProcessSubjectPrefix),
		},
		Retention:         jetstream.WorkQueuePolicy,
		Discard:           jetstream.DiscardOld,
		MaxMsgsPerSubject: 1,
		AllowMsgSchedules: true,
		AllowMsgTTL:       true,
		Replicas:          1,
	})
	if err != nil {
		return err
	}

	log.Println("스트림생성: ",
		fmt.Sprintf("%s.*", pendingSubjectPrefix),
		fmt.Sprintf("%s.*", onProcessSubjectPrefix))

	natsClient.Stream = stream

	streamInfo, err := natsClient.JetStream.Stream(context.Background(), StreamName)
	if err != nil {
		return err
	}

	log.Printf("📢Stream state: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)

	return nil
}

func createSchedulerConsumer() error {
	consumer, err := natsClient.Stream.CreateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:        "scheduledEventSinkConsumer",
		FilterSubjects: []string{fmt.Sprintf("%s.*", onProcessSubjectPrefix)},
		AckPolicy:      jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	natsClient.Consumer = consumer

	ctxList := make([]jetstream.ConsumeContext, 0)

	for range 3 {
		consumeCtx, err := natsClient.Consumer.Consume(func(msg jetstream.Msg) {

			defer msg.Ack()

			var content MessageContent
			err := json.Unmarshal(msg.Data(), &content)
			if err != nil {
				log.Fatalf("failed to unmarshal: %v", err)
			}

			latency := time.Since(content.ScheduledAt)

			// [FIX 3] 음수 latency는 0으로 처리 (이미 예약 시각 전에 도착 = 오차 없음으로 간주)
			if latency < 0 {
				latency = 0
			}

			latencyChan <- latency

			// [FIX 4] P99 계산용 slice에 추가
			latenciesMu.Lock()
			latencies = append(latencies, latency)
			latenciesMu.Unlock()

			wg.Done()
			log.Printf("calculated latency: now: %v, scheduledAt : %v, Latency: %v", time.Now(), content.ScheduledAt, latency)
		})
		if err != nil {
			return err
		}
		ctxList = append(ctxList, consumeCtx)
	}

	natsClient.ConsumeContextList = ctxList

	return nil
}

func latencyAggregator() {
	defer latencyAggregatorWg.Done()

	for latency := range latencyChan {
		log.Println("latency", latency)

		if latency > maxLatency {
			maxLatency = latency
			log.Printf("🚨 max Latency updated: %s", maxLatency)
		}

		totalLatency += latency
		messageCount++
	}
}

func percentileFromSorted(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return sorted[idx]
}

type MessageContent struct {
	Id          string
	ScheduledAt time.Time
}

// [FIX 2] 반환값을 성공한 publish 건수로 변경
func publishScheduledMessageToSchedulerStream() int {
	successCount := 0

	for idx := range NumScheduledMessages {

		var scheduleId = fmt.Sprintf("SCH_ID_%d", idx+1)

		fastestScheduledAt := getFastestScheduledAtbyIdx(idx)

		for mc := 1; mc <= ModifyCountPerMessage; mc++ {

			scheduledAt := fastestScheduledAt.Add(time.Duration(InitDelayTime-mc) * time.Minute)

			if idx == 0 && mc == ModifyCountPerMessage {
				FirstScheduledAt = scheduledAt
			}

			if idx == NumScheduledMessages-1 && mc == ModifyCountPerMessage {
				LastScheduledAt = scheduledAt
			}

			remainingTime := int(time.Until(scheduledAt).Seconds())

			msg := MessageContent{
				Id:          scheduleId,
				ScheduledAt: scheduledAt,
			}

			msgBytes, err := json.Marshal(msg)
			if err != nil {
				log.Printf("failed to marshal: %v", err)
				continue
			}

			pubAck, err := natsClient.JetStream.PublishMsg(context.Background(), &nats.Msg{
				Header: nats.Header{
					"Nats-Schedule":        []string{fmt.Sprintf("@at %s", scheduledAt.Format(time.RFC3339))},
					"Nats-Schedule-TTL":    []string{"180s"},
					"Nats-Schedule-Target": []string{fmt.Sprintf("%s.%s", onProcessSubjectPrefix, scheduleId)},
				},
				Subject: fmt.Sprintf("%s.%s", pendingSubjectPrefix, scheduleId),
				Data:    msgBytes,
			})
			if err != nil {
				log.Printf("failed to publish msg to scheduler stream: %v, remainingTime: %d", err, remainingTime)
				// [FIX 2] 최종 수정본(mc == ModifyCountPerMessage)이 실패한 경우만 카운트 감소
				if mc == ModifyCountPerMessage {
					publishFailMu.Lock()
					publishFailCount++
					publishFailMu.Unlock()
				}
				continue
			}

			if mc == ModifyCountPerMessage {
				successCount++
				log.Println("targetSubject: ", fmt.Sprintf("%s.%s", onProcessSubjectPrefix, scheduleId))
				log.Printf("final version of scheduled msg(%s) published: %+v, scheduledAt: %s (remaining %ds)", scheduleId, pubAck, scheduledAt.Format(time.RFC3339), remainingTime)
			}
		}
	}

	PrintStreamInfo()
	return successCount
}

func getFastestScheduledAtbyIdx(idx int) time.Time {

	fatestNextMinuteTime := getFatestNextMinuteTime()

	d := idx / ScheduledMsgGroupingCount

	add := ScheduleTimeIntervalMin * (d + 1)

	return fatestNextMinuteTime.Add(time.Duration(add) * time.Minute)
}

func getFatestNextMinuteTime() time.Time {
	currentTime := time.Now()
	currentSec := currentTime.Second()
	currentTimeWithoutSec := currentTime.Add(time.Duration(-currentSec) * time.Second)
	return currentTimeWithoutSec.Add(1 * time.Minute)
}

func PrintStreamInfo() {
	streamInfo, err := natsClient.JetStream.Stream(context.Background(), StreamName)
	if err == nil {
		log.Printf("📢 Stream state: Messages: %d, Bytes: %d", streamInfo.CachedInfo().State.Msgs, streamInfo.CachedInfo().State.Bytes)
	}
}
