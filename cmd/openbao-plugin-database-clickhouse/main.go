// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"log"
	"os"

	clickhouse "github.com/elaunira/openbao-plugin-database-clickhouse"
	"github.com/openbao/openbao/sdk/v2/database/dbplugin/v5"
)

var (
	version = "dev"
)

func main() {
	err := Run()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

// Run instantiates a clickhouse object and runs the RPC server for the plugin.
func Run() error {
	f := clickhouse.New(clickhouse.DefaultUserNameTemplate(), version)

	dbplugin.ServeMultiplex(f)

	return nil
}
