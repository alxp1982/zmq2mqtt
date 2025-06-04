package main

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.uber.org/zap"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gorilla/websocket"
	zmq "github.com/pebbe/zmq4"
	"github.com/tkanos/gonfig"
	"golang.org/x/exp/slices"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan map[string]interface{})

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	clients[conn] = true
	defer delete(clients, conn)

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func broadcastStats(stats *Stats) {
	for {
		time.Sleep(1 * time.Second)
		statsData := stats.GetStats()
		broadcast <- statsData
	}
}

func forwarder_thread(logger *zap.SugaredLogger, config *Configuration, stats *Stats, cancel chan bool) {
	logger.Debugln("enter forward_thread")
	defer logger.Debugln("exit forward_thread")

	pipe := setupZeroMQSocket(logger)

	defer pipe.Close()

	client := setupMQTTClient(logger, config)

	defer client.Disconnect(200)

	poller := zmq.NewPoller()
	poller.Add(pipe, zmq.POLLIN)

	for {
		switch sockets, err := poller.Poll(100 * time.Millisecond); {
		case err != nil:
			logger.Errorln(err)
			stats.UpdateConnectionStatus(true, false)
		case len(sockets) > 0:
			stats.UpdateConnectionStatus(true, client.IsConnected())

			if !client.IsConnected() {
				logger.Warnln("MQTT not connected. Skipping message")
				continue
			}

			for _, socket := range sockets {
				if socket.Events&zmq.POLLIN != 0 {
					msgs, err := pipe.RecvMessage(0)

					if err != nil {
						logger.Errorln(err)
						continue
					}

					logger.Debugf("Received messages %d", len(msgs))
					stats.IncrementMessages(true, false)

					for _, msg := range msgs {
						topic := strings.Split(msg, " ")[0]

						if len(msg) > len(topic) {
							payload := msg[len(topic):]

							if slices.IndexFunc(config.ForwardTopics, func(s string) bool {
								return s == topic
							}) != -1 {
								logger.Debugf("Forwarding message %s", payload)
								token := client.Publish(topic, 1, false, payload)

								if token.Error() != nil {
									logger.Warnln(token.Error().Error())
								} else {
									stats.IncrementMessages(false, true)
								}
							}
						}
					}
				}
			}
		}

		select {
		case <-cancel:
			logger.Debugln("cancellation signal received")
			return
		default:
		}
	}
}

func setupZeroMQSocket(logger *zap.SugaredLogger) *zmq.Socket {
	pipe, err := zmq.NewSocket(zmq.PAIR)

	if err != nil {
		logger.Error(err)
		//communicate error to main thread
		return nil
	}

	err = pipe.Bind("inproc://pipe")

	if err != nil {
		logger.Error(err)
		//communicate error to main thread
		return nil
	}

	logger.Debugln("ZeroMQ socket setup completed")
	return pipe
}

func setupMQTTClient(logger *zap.SugaredLogger, config *Configuration) mqtt.Client {
	opts := mqtt.NewClientOptions().AddBroker(config.MqttServer).SetClientID(config.MqttClientId)

	opts.SetKeepAlive(time.Duration(config.MqttConfig.KeepAliveTimeout * int(time.Second)))
	opts.SetPingTimeout(time.Duration(config.MqttConfig.PingTimeout * int(time.Second)))
	opts.SetMaxReconnectInterval(time.Duration(config.MqttConfig.MaxReconnectInterval * int(time.Second)))
	opts.AutoReconnect = config.MqttConfig.AutoReconnect

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		logger.Warnf("!!!!!! mqtt connection lost error: %s\n", err.Error())
	})

	opts.SetReconnectingHandler(func(c mqtt.Client, options *mqtt.ClientOptions) {
		logger.Infoln("mqtt reconnecting.....")
	})

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		logger.Infoln("mqtt connected.....")
	})

	client := mqtt.NewClient(opts)

	for token := client.Connect(); token.Wait() && token.Error() != nil; {
		logger.Warnf("failed to connect to MQTT. Error:%v. Retrying in %d sec", token.Error(), 10)
		time.Sleep(10 * time.Second)
	}

	logger.Debugln("MQTT client setup completed")
	return client
}

func main() {
	//load configuration
	configuration := Configuration{}
	err := gonfig.GetConf(getConfigFileName(), &configuration)

	if err != nil {
		panic(err)
	}

	logger := zap.Must(configuration.LoggerConfig.Build())
	defer logger.Sync()

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	sugar := logger.Sugar()

	sugar.Infoln("Command line arguments:")
	sugar.Infof("Front-end port: %d\n", configuration.FrontendPort)
	sugar.Infof("Back-end port: %d\n", configuration.BackendPort)
	sugar.Infof("MQTT Server: %s\n", configuration.MqttServer)
	sugar.Infof("Topics:%v\n", configuration.ForwardTopics)

	cancel := make(chan bool)

	stats := NewStats(100) // 100 message window for rate calculation

	go forwarder_thread(sugar, &configuration, stats, cancel)
	defer func() { cancel <- true }()

	// Start web server in a goroutine
	go func() {
		http.HandleFunc("/", handleDashboard)
		http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			handleStats(w, r, stats)
		})
		http.HandleFunc("/ws", handleWebSocket)

		// Start broadcasting stats
		go broadcastStats(stats)

		// Handle broadcasting to clients
		go func() {
			for {
				statsData := <-broadcast
				for client := range clients {
					err := client.WriteJSON(statsData)
					if err != nil {
						client.Close()
						delete(clients, client)
					}
				}
			}
		}()

		err := http.ListenAndServe(fmt.Sprintf(":%d", configuration.WebPort), nil)
		if err != nil {
			sugar.Errorln("Web server error:", err)
		}
	}()

	//Setup proxy
	subscriber, _ := zmq.NewSocket(zmq.XSUB)
	subscriber_addr := fmt.Sprintf("tcp://*:%d", configuration.FrontendPort)
	subscriber.Bind(subscriber_addr)

	publisher, _ := zmq.NewSocket(zmq.XPUB)
	publisher.Bind(fmt.Sprintf("tcp://*:%d", configuration.BackendPort))

	listener, _ := zmq.NewSocket(zmq.PAIR)
	listener.Connect("inproc://pipe")

	zmq.Proxy(subscriber, publisher, listener)
}

func getConfigFileName() string {
	env := os.Getenv("ENV")
	if len(env) == 0 {
		env = "development"
	}
	filename := []string{"config/", "config.", env, ".json"}
	_, dirname, _, _ := runtime.Caller(0)
	filePath := path.Join(filepath.Dir(dirname), strings.Join(filename, ""))

	return filePath
}
