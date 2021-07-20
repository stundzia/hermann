package config

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"sync"
)

type Config struct {
	Kafka *kafka
}

type kafka struct {
	Address string
	Topic string
	Topics []string
}


var configPath = "conf"
var configInstance *Config
var once sync.Once

func GetConfig() *Config {
	once.Do(func() {
		setConfig(zap.NewNop())
	})
	return configInstance
}

func setConfig(logger *zap.Logger) {
	config := viper.New()
	config.SetConfigName("base")
	config.AddConfigPath(configPath)
	err := config.ReadInConfig() // Find and read the config file
	if err != nil {              // Handle errors reading the config file
		logger.Fatal("couldn't read config file", zap.Error(err))
	}

	err = config.Unmarshal(&configInstance)
	if err != nil {
		panic(err)
	}
}