package cfg

import (
	"io/ioutil"
	"os"
	"path/filepath"

	yaml "gopkg.in/yaml.v2"
)

type CfgTag struct {
	Tag  string `yaml:"Tag"`
	Type string `yaml:"Type"`
}

type Config struct {
	Influx           bool           `yaml:"Influx"`
	InfluxURL        string         `yaml:"InfluxURL"`
	InfuxAPIToken    string         `yaml:"InfluxAPIToken"`
	InfluxOrgName    string         `yaml:"InfluxOrgName"`
	InfluxBucketName string         `yaml:"InfluxBucketName"`
	FlukeTags        map[int]CfgTag `yaml:"FlukeTags"`
}

var (
	configFileName = "fluke.yaml"
)

// InitConfig initializes the config from the config YAML file
func InitConfig() (*Config, error) {
	// use current working directory to try and find config file
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	// open the config file
	cfgBytes, err := ioutil.ReadFile(filepath.Join(cwd, configFileName))
	if err != nil {
		return nil, err
	}
	// marshall into YAML
	var cfg Config
	err = yaml.Unmarshal(cfgBytes, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
