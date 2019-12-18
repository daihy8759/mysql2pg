/*
Copyright © 2019 NAME HERE <EMAIL ADDRESS>

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
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var log = logrus.New()
var cfgFile string
var srcDb *sql.DB
var destDb *sql.DB

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mysql2pg",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		mysql2pg()
	},
}

func mysql2pg() {
	start := time.Now()
	log.Info("检查Mysql连接")
	PrepareSrc()
	defer srcDb.Close()

	log.Info("检查Postgres连接")
	PrepareDest()
	defer destDb.Close()
	tables := viper.GetStringMapStringSlice("tables")
	// maxRows := viper.GetInt("maxRows")
	var goroutineSize int
	for _, sqlList := range tables {
		goroutineSize += len(sqlList)
	}
	ch := make(chan int, goroutineSize)
	for table, sqlList := range tables {
		for index, v := range sqlList {
			go runMigration(index, table, v, ch)
		}
	}
	for i := 0; i < goroutineSize; i++ {
		<-ch
	}
	cost := time.Since(start)
	log.Info(fmt.Sprintf("执行完成，耗时%s", cost))
}

func runMigration(index int, table string, sqlstr string, ch chan int) {
	log.Info(fmt.Sprintf("goroutine[%d]开始执行", index))
	start := time.Now()
	rows, err := srcDb.Query(sqlstr)
	defer rows.Close()
	if err != nil {
		log.Error("Query failed,err:%v\n", err)
		return
	}
	columns, _ := rows.Columns()
	if err != nil {
		log.Fatal(err.Error())
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	txn, err := destDb.Begin()
	stmt, err := txn.Prepare(pq.CopyIn(table, columns...))
	if err != nil {
		log.Fatal(err)
	}
	var totalRow int
	for rows.Next() {
		totalRow++
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Fatal(err.Error())
		}
		var value interface{}
		prepareValues := make([]interface{}, len(columns))

		for i, col := range values {
			if col == nil {
				value = nil
			} else {
				value = string(col)
			}
			prepareValues[i] = value
		}
		_, err = stmt.Exec(prepareValues...)
		if err != nil {
			log.Fatal(err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	cost := time.Since(start)
	log.Info(fmt.Sprintf("goroutine[%d]执行完成,迁移%d行,耗时%s", index, totalRow, cost))
	ch <- 1
}

/**
* 连接Mysql数据库
**/
func PrepareSrc() {
	srcHost := viper.GetString("src.host")
	srcUserName := viper.GetString("src.username")
	srcPassword := viper.GetString("src.password")
	srcDatabase := viper.GetString("src.database")
	srcConn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&maxAllowedPacket=0", srcUserName, srcPassword, srcHost, srcDatabase)
	var err error
	srcDb, err = sql.Open("mysql", srcConn)
	if err != nil {
		log.Fatal("请检查Mysql配置", err)
	}
	c := srcDb.Ping()
	if c != nil {
		log.Fatal("连接Mysql失败", err)
	}
	srcDb.SetConnMaxLifetime(2 * time.Hour)
	srcDb.SetMaxIdleConns(0)
	srcDb.SetMaxOpenConns(30)
	log.Info("连接Mysql成功")
}

/**
* 连接Postgres数据库
**/
func PrepareDest() {
	destHost := viper.GetString("dest.host")
	destUserName := viper.GetString("dest.username")
	destPassword := viper.GetString("dest.password")
	destDatabase := viper.GetString("dest.database")

	conn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", destHost,
		destUserName, destPassword, destDatabase)
	var err error
	destDb, err = sql.Open("postgres", conn)
	if err != nil {
		log.Fatal("请检查Postgres配置", err)
	}
	c := destDb.Ping()
	if c != nil {
		log.Fatal("连接Postgres失败", err)
	}
	log.Info("连接Postgres成功")
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
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mysql2pg.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".mysql2pg" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".mysql2pg")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	}
}
