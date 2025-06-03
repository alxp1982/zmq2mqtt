package main

import (
	"go.uber.org/zap"
)

type MqttConfiguration struct {
	KeepAliveTimeout     int  //MQTT keep alive interval, sec
	PingTimeout          int  //MQTT ping timeout interval, sec
	MaxReconnectInterval int  //MQTT max reconnect interval, sec
	AutoReconnect        bool //whether MQTT should auto reconnect
}

type Configuration struct {
	FrontendPort int `env:FE_PORT` //ZMQ frontend port
	BackendPort  int `env:BE_PORT` //ZMQ backend port

	MqttServer string `env:MQTT_SERVER` //MQTT server address to forward messages

	MqttConfig MqttConfiguration

	MqttClientId  string
	ForwardTopics []string //ZMQ topics to forward to MQTT
	LoggerConfig  zap.Config
	WebPort       int // Web server port
}
