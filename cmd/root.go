/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/peterepsteen/hey-kafka/app"

	"github.com/spf13/viper"

	"github.com/spf13/cobra"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hey-kafka",
	Short: "Dispatch binary encoded avro messages to Kafka",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		appConf := getConf()
		if err := app.Run(appConf); err != nil {
			log.Fatalf("Error in app run: %s", err.Error())
		} else {
			log.Println("Success")
		}
	},
}

func getConf() *app.Config {
	appConf := &app.Config{}
	if path := viper.GetString("file"); path != "" {
		viper.SetConfigFile(path)
		viper.ReadInConfig()
	}
	if err := viper.Unmarshal(appConf); err != nil {
		log.Fatalf("Error in marshaling config: %s", err.Error())
	}
	return appConf
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("schema", "s", "", `
A filepath to the avro schema you wish to use
  `)

	rootCmd.PersistentFlags().StringP("message", "m", "", `
Either the message to send in string form, or a filepath containing the message you wish to send
  `)

	rootCmd.PersistentFlags().StringP("host", "H", "localhost", `
The host that kafka is listening on
  `)

	rootCmd.PersistentFlags().IntP("port", "P", 9092, `
The port that kafka is listening on
  `)

	rootCmd.PersistentFlags().StringP("topic", "t", "", `
The topic to send the message to. This is also used for the subject of the schema registry, if configured.
  `)

	rootCmd.PersistentFlags().String("schema-registry-address", "", `
The address of the Schema Registry to use. IE localhost:8081
  `)

	rootCmd.PersistentFlags().StringP("file", "f", "", `
Optional path to a config file to use. Command line options take precedence
  `)

	viper.BindPFlags(rootCmd.PersistentFlags())
}
