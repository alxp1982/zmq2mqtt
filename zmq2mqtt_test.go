package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	zmq "github.com/pebbe/zmq4"
	"github.com/tkanos/gonfig"
)

const (
	MQTT_DOCKER_IMG_NAME         = "eclipse-mosquitto"
	BENCHMARK_NUMBER_OF_MESSAGES = 1000
)

func TestMqttConnectPublish(t *testing.T) {
	ctr_id, err := initMqtt()
	defer removeMqtt(ctr_id)

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup", "Error starting MQTT: %v", err)
	}

	logger, config, zmq_socket, err := setup()

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup: %v", "Setup completed", err)
	}

	cancel := make(chan bool)

	stats := NewStats(100) // 100 message window for rate calculation
	go forwarder_thread(logger, config, stats, cancel)

	defer func() {
		//close forwarder thread
		cancel <- true
	}()

	var message_received bool = false

	//send message through listener, make sure it is published to MQTT
	opts := mqtt.NewClientOptions().AddBroker(config.MqttServer).SetClientID("test client")
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetMaxReconnectInterval(30 * time.Second)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		assert.FailNowf(t, "Error connecting to MQTT %v", "Setup completed", token.Error())
	}

	defer client.Disconnect(250)

	//subscribe to recognition topic
	topic := "testtopic"
	token := client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
		message_received = true
	})
	token.Wait()

	//publish message through zmq
	zmq_socket.Send("testtopic test message", 0)

	timeout := time.After(15 * time.Second)

	//wait for message received with timeout
	for {
		select {
		case <-timeout:
			assert.FailNow(t, "Message isn't received in MQTT")
		default:
			if message_received {
				return
			}
		}
	}
}

func TestMqttPublishAfterReconnect(t *testing.T) {
	ctr_id, err := initMqtt()
	defer removeMqtt(ctr_id)

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup", "Error starting MQTT: %v", err)
	}

	logger, config, zmq_socket, err := setup()

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup: %v", "Setup completed", err)
	}

	cancel := make(chan bool)

	stats := NewStats(100) // 100 message window for rate calculation
	go forwarder_thread(logger, config, stats, cancel)

	defer func() {
		//close forwarder thread
		cancel <- true
	}()

	var message_received bool = false

	opts := mqtt.NewClientOptions().AddBroker(config.MqttServer).SetClientID("test client")
	opts.SetKeepAlive(5 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetMaxReconnectInterval(30 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		assert.FailNowf(t, "Error connecting to MQTT %v", "Setup completed", token.Error())
	}

	defer client.Disconnect(250)

	//subscribe to recognition topic
	topic := "testtopic"
	token := client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
		message_received = true
	})
	token.Wait()

	//stop MQTT
	if err = stopMqtt(ctr_id); !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in stopping MQTT container: %v", "Stopping MQTT", err)
		return
	}

	//start MQTT
	if err = startMqtt(ctr_id); !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in starting MQTT container: %v", "Starting MQTT", err)
		return
	}

	//publish message through zmq while MQTT is stopped
	zmq_socket.Send("testtopic test message", 0)

	//wait for message received with timeout
	timeout := time.After(15 * time.Second)

	for {
		select {
		case <-timeout:
			assert.FailNow(t, "Message isn't received in MQTT")
			return
		default:
			if message_received {
				return
			}
		}
	}
}

func TestMqttPublishDuringReconnect(t *testing.T) {
	ctr_id, err := initMqtt()
	defer removeMqtt(ctr_id)

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup", "Error starting MQTT: %v", err)
		return
	}

	logger, config, zmq_socket, err := setup()

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup: %v", "Setup completed", err)
		return
	}

	cancel := make(chan bool)

	stats := NewStats(100) // 100 message window for rate calculation
	go forwarder_thread(logger, config, stats, cancel)
	defer func() {
		//close forwarder thread
		cancel <- true
	}()

	var message_received bool = false

	opts := mqtt.NewClientOptions().AddBroker(config.MqttServer).SetClientID("test client")
	opts.SetKeepAlive(5 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetMaxReconnectInterval(30 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		assert.FailNowf(t, "Error connecting to MQTT %v", "Setup completed", token.Error())
		return
	}

	defer client.Disconnect(250)

	//subscribe to recognition topic
	topic := "testtopic"
	token := client.Subscribe(topic, 1, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
		message_received = true
	})
	token.Wait()

	//stop MQTT
	if err = stopMqtt(ctr_id); !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in stopping MQTT container: %v", "Stopping MQTT", err)
		return
	}

	//publish message through zmq while MQTT is stopped
	zmq_socket.Send("testtopic test message", 0)

	//start MQTT
	if err = startMqtt(ctr_id); !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in starting MQTT container: %v", "Starting MQTT", err)
		return
	}

	timeout := time.After(15 * time.Second)
	//wait for message received with timeout
	for {
		select {
		case <-timeout:
			assert.FailNow(t, "Message isn't received in MQTT")
			return
		default:
			if message_received {
				return
			}
		}
	}
}

func TestLatency(t *testing.T) {
	//Test latency by first measuring average latency sending 100 messages to MQTT directly
	//After that, sending 100 messages through ZMQ->MQTT bridge

	ctr_id, err := initMqtt()
	defer removeMqtt(ctr_id)

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup", "Error starting MQTT: %v", err)
		return
	}

	logger, config, zmq_socket, err := setup()

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup: %v", "Setup completed", err)
		return
	}

	cancel := make(chan bool)

	stats := NewStats(100) // 100 message window for rate calculation
	go forwarder_thread(logger, config, stats, cancel)
	defer func() {
		//close forwarder thread
		cancel <- true
	}()

	opts := mqtt.NewClientOptions().AddBroker(config.MqttServer).SetClientID("test client")
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetMaxReconnectInterval(30 * time.Second)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		assert.FailNowf(t, "Error connecting to MQTT %v", "Setup completed", token.Error())
	}

	messages_received := 0

	//subscribe to recognition topic
	topic := "testtopic"
	token := client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		//fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
		messages_received++
	})
	token.Wait()

	starttime := time.Now()

	for i := 0; i < BENCHMARK_NUMBER_OF_MESSAGES; i++ {
		client.Publish("testtopic", 1, false, "Test Message")
	}

	timeout := time.After(5 * time.Second)
	//wait for message received with timeout
	for messages_received < BENCHMARK_NUMBER_OF_MESSAGES {
		select {
		case <-timeout:
			assert.FailNow(t, "Timeout: message isn't received in MQTT")
			return
		default:
		}
	}

	duration_mqtt := time.Since(starttime)

	//now send messages through ZMQ->MQTT bridge
	messages_received = 0

	starttime = time.Now()

	for i := 0; i < BENCHMARK_NUMBER_OF_MESSAGES; i++ {
		zmq_socket.Send("testtopic test message", 0)
	}

	timeout = time.After(5 * time.Second)
	//wait for message received with timeout
	for messages_received < BENCHMARK_NUMBER_OF_MESSAGES {
		select {
		case <-timeout:
			assert.FailNow(t, "Message isn't received in MQTT")
			return
		default:
		}
	}

	duration_zmq := time.Since(starttime)

	fmt.Printf("Benchmark latency: %f, ms\n", duration_mqtt.Seconds()/float64(BENCHMARK_NUMBER_OF_MESSAGES)*1000)
	fmt.Printf("ZMQ->MQTT latency: %f, ms\n", duration_zmq.Seconds()/float64(BENCHMARK_NUMBER_OF_MESSAGES)*1000)

}

func TestDashboardParameters(t *testing.T) {
	ctr_id, err := initMqtt()
	defer removeMqtt(ctr_id)

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup", "Error starting MQTT: %v", err)
	}

	logger, config, zmq_socket, err := setup()

	if !assert.NoError(t, err) {
		assert.FailNowf(t, "Error in setup: %v", "Setup completed", err)
	}

	cancel := make(chan bool)

	stats := NewStats(100) // 100 message window for rate calculation
	go forwarder_thread(logger, config, stats, cancel)

	defer func() {
		//close forwarder thread
		cancel <- true
	}()

	// Test connection status
	opts := mqtt.NewClientOptions().AddBroker(config.MqttServer).SetClientID("test client")
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetMaxReconnectInterval(30 * time.Second)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		assert.FailNowf(t, "Error connecting to MQTT %v", "Setup completed", token.Error())
	}

	defer client.Disconnect(250)

	// Wait for connection to be established
	time.Sleep(2 * time.Second)

	// Check connection status
	statsData := stats.GetStats()
	assert.True(t, statsData["mqtt_connected"].(bool), "MQTT should be connected")

	// Test message counters
	topic := "testtopic"
	token := client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	})
	token.Wait()

	// Publish message through zmq
	zmq_socket.Send("testtopic test message", 0)

	// Wait for message to be processed
	time.Sleep(2 * time.Second)

	// Check message counters
	statsData = stats.GetStats()
	assert.Equal(t, int64(1), statsData["messages_received"].(int64), "Messages received should be 1")
	assert.Equal(t, int64(1), statsData["messages_forwarded"].(int64), "Messages forwarded should be 1")

	// Test message rate
	// Publish more messages to calculate rate
	for i := 0; i < 10; i++ {
		zmq_socket.Send("testtopic test message", 0)
	}

	// Wait for messages to be processed
	time.Sleep(2 * time.Second)

	// Check message rate
	statsData = stats.GetStats()
	assert.Greater(t, statsData["message_rate"].(float64), 0.0, "Message rate should be greater than 0")
}

func setup() (*zap.SugaredLogger, *Configuration, *zmq.Socket, error) {
	fmt.Println("in setup")

	//load configuration
	configuration := Configuration{}
	err := gonfig.GetConf("config/config.test.json", &configuration)

	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, err
	}

	//setup logging
	logger := zap.Must(configuration.LoggerConfig.Build())
	defer logger.Sync()

	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, err
	}

	sugar := logger.Sugar()

	//setup listener socket
	listener, err := zmq.NewSocket(zmq.PAIR)

	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, err
	}

	err = listener.Connect("inproc://pipe")

	if err != nil {
		fmt.Println(err)
		return nil, nil, nil, err
	}

	return sugar, &configuration, listener, nil
}

func initMqtt() (string, error) {
	ctr_idx := ""
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

	if err != nil {
		return ctr_idx, err
	}

	defer cli.Close()

	mosquitto_cfg_path, err := filepath.Abs("test_data/mosquitto/mosquitto.config")

	if err != nil {
		return ctr_idx, err
	}

	ctr_config := &container.Config{
		Image: MQTT_DOCKER_IMG_NAME,
		ExposedPorts: nat.PortSet{
			"1883/tcp": struct{}{},
			"9001/tcp": struct{}{},
		},
		Volumes: map[string]struct{}{},
	}

	host_config := &container.HostConfig{
		PortBindings: nat.PortMap{
			"1883/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "1883",
				},
			},
			"9001/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "9001",
				},
			},
		},

		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: mosquitto_cfg_path,
				Target: "/mosquitto/config/mosquitto.conf",
			},
		},
	}

	resp, err := cli.ContainerCreate(ctx, ctr_config, host_config, nil, nil, "")

	if err != nil {
		return resp.ID, err
	}

	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return resp.ID, err
	}

	return resp.ID, nil
}

func startMqtt(id string) error {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

	if err != nil {
		return err
	}

	defer cli.Close()

	if err := cli.ContainerUnpause(ctx, id); err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	return nil
}

func stopMqtt(id string) error {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

	if err != nil {
		return err
	}

	defer cli.Close()

	if err := cli.ContainerPause(ctx, id); err != nil {
		return err
	}

	return nil
}

func removeMqtt(id string) error {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

	if err != nil {
		return err
	}

	defer cli.Close()

	err = cli.ContainerStop(ctx, id, container.StopOptions{})

	if err != nil {
		return err
	}

	cli.ContainerRemove(ctx, id, types.ContainerRemoveOptions{})

	return nil
}
