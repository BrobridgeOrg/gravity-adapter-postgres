package instance

import (
	"fmt"

	gravity_adapter "github.com/BrobridgeOrg/gravity-sdk/v2/adapter"
	"github.com/BrobridgeOrg/gravity-sdk/v2/core"

	//"github.com/BrobridgeOrg/gravity-sdk/core/keyring"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	DefaultPingInterval        = 10
	DefaultMaxPingsOutstanding = 3
	DefaultMaxReconnects       = -1
)

func (a *AppInstance) initAdapterConnector() error {

	// default settings
	viper.SetDefault("gravity.domain", "gravity")
	viper.SetDefault("gravity.pingInterval", DefaultPingInterval)
	viper.SetDefault("gravity.maxPingsOutstanding", DefaultMaxPingsOutstanding)
	viper.SetDefault("gravity.maxReconnects", DefaultMaxReconnects)
	viper.SetDefault("gravity.accessToken", "")

	// Read configs
	domain := viper.GetString("gravity.domain")
	host := viper.GetString("gravity.host")
	port := viper.GetInt("gravity.port")
	pingInterval := viper.GetInt64("gravity.pingInterval")
	maxPingsOutstanding := viper.GetInt("gravity.maxPingsOutstanding")
	maxReconnects := viper.GetInt("gravity.maxReconnects")
	accessToken := viper.GetString("gravity.accessToken")

	// Preparing options
	options := core.NewOptions()
	options.PingInterval = time.Duration(pingInterval) * time.Second
	options.MaxPingsOutstanding = maxPingsOutstanding
	options.MaxReconnects = maxReconnects
	options.Token = accessToken

	address := fmt.Sprintf("%s:%d", host, port)

	log.WithFields(log.Fields{
		"address":             address,
		"pingInterval":        options.PingInterval,
		"maxPingsOutstanding": options.MaxPingsOutstanding,
		"maxReconnects":       options.MaxReconnects,
	}).Info("Connecting to gravity...")

	// Connect to gravity
	client := core.NewClient()
	err := client.Connect(address, options)
	if err != nil {
		return err
	}

	// Initializing gravity adapter connector
	opts := gravity_adapter.NewOptions()
	opts.Domain = domain

	a.adapterConnector = gravity_adapter.NewAdapterConnectorWithClient(client, opts)
	err = a.adapterConnector.Connect(address, options)
	if err != nil {
		return err
	}

	/*
		// Loading access key
		viper.SetDefault("adapter.appID", "anonymous")
		viper.SetDefault("adapter.accessKey", "")
		opts.Key = keyring.NewKey(viper.GetString("adapter.appID"), viper.GetString("adapter.accessKey"))

		a.adapterConnector = gravity_adapter.NewAdapterConnectorWithClient(client, opts)

		// Register adapter
		adapterID := viper.GetString("adapter.adapterID")
		adapterName := viper.GetString("adapter.adapterName")

		log.WithFields(log.Fields{
			"id":   adapterID,
			"name": adapterName,
		}).Info("Registering adapter")

		err = a.adapterConnector.Register("postgres", adapterID, adapterName)
		if err != nil {
			return err
		}
	*/

	return nil
}

func (a *AppInstance) GetAdapterConnector() *gravity_adapter.AdapterConnector {
	return a.adapterConnector
}
