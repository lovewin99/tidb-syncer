package river

import (
	"io/ioutil"
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

/**
	Match the configuration file
 */

type FromConfig struct {
	Host string `toml:"host"`
	User string `toml:"user"`
	Password string `toml:"password"`
	Port int64 `toml:"port"`
}

type ToConfig struct {
	Host string `toml:"host"`
	User string `toml:"user"`
	Password string `toml:"password"`
	Port int64 `toml:"port"`
}

type SctDoTable struct {
	DbName string `toml:"db-name"`
	TbName string `toml:"tbl-name"`
}

type SctIgnoreTable struct {
	DbName string `toml:"db-name"`
	TbName string `toml:"tbl-name"`
}

type SkipDml struct {
	DbName string `toml:"db-name"`
	TbName string `toml:"tbl-name"`
	Type string `toml:"type"`
}

type Config struct {
	// TODO add more config
	Fromcfg	FromConfig `toml:"from"`
	Tocfg	ToConfig `toml:"to"`
	DataDir	string `toml:"meta"`
	ServerID	uint32 `toml:"server-id"`
	StatAddr	string `toml:"status-addr"`

	LogLever	string `toml:"log-level"`
	LogPath		string `toml:"log-file"`
	LogRotate	string
	LogConsole	bool


	WorkerCount	int `toml:"worker-count"`
	Batch 		int `toml:"batch"`

	DoDB	[]string	`toml:"replicate-do-db"`
	DoTable	[]SctDoTable	`toml:"replicate-do-table"`
	IgnoreDB	[]string	`toml:"replicate-ignore-db"`
	IgnoreTable	[]SctIgnoreTable	`toml:"replicate-ignore-table"`
	RouteRules	[]Rule	`toml:"route-rules"`

	SkipDmls	[]SkipDml `toml:"skip-dmls"`
	SkipDdl		[]string `toml:"skip-ddls"`
}

func defaultConfig(c *Config) {
	c.LogLever = "info"
	c.Batch = 10
	c.WorkerCount = 16
	c.LogPath = "./syncer.log"
	c.LogRotate = "day"
	c.DataDir = "./syncer.meta"
	c.ServerID = 101
	c.StatAddr = "127:0.0.1:10088"
	c.LogConsole = false

}

// NewConfigWithFile creates a Config from file.
func NewConfigWithFile(name string) (*Config, error) {

	var c Config
	defaultConfig(&c)
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = NewConfig(&c, string(data))
	return &c, err
}

// NewConfig creates a Config from data.
func NewConfig(c *Config, data string) error {

	_, err := toml.Decode(data, c)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}