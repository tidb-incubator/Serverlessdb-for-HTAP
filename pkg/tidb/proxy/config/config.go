// Copyright 2016 The he3proxy Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

//用于通过api保存配置
var configFileName string

//整个config文件对应的结构
type Config struct {
	WebAddr     string `yaml:"web_addr"`
	WebUser     string `yaml:"web_user"`
	WebPassword string `yaml:"web_password"`

	SlowLogTime int    `yaml:"slow_log_time"`
	AllowIps    string `yaml:"allow_ips"`

	Charset string        `yaml:"proxy_charset"`
	Cluster ClusterConfig `yaml:"clusters"`
}

//user_list对应的配置
type UserConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

//node节点对应的配置
type ClusterConfig struct {
	ClusterName      string `yaml:"clustername"`
	NameSpace        string `yaml:"namespace"`
	DownAfterNoAlive int    `yaml:"down_after_noalive"`
	//for serverless
	ServerlessAddr    string `yaml:"serverless_addr"`
	ResendForScaleOUT int    `yaml:"resend_for_scale_out"`
	ScaleInInterval   int    `yaml:"scale_in_interval"`
	SilentPeriod      int    `yaml:"silent_period"`

	User     string `yaml:"user"`
	Password string `yaml:"password"`

	Tidbs string `yaml:"tidbs"`
}

func ParseConfigData(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ParseConfigFile(fileName string) (*Config, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	configFileName = fileName

	return ParseConfigData(data)
}

func WriteConfigFile(cfg *Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(configFileName, data, 0755)
	if err != nil {
		return err
	}

	return nil
}
