// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

// Package main provides the entrypoint for the ClickHouse database plugin.
package main

import (
	clickhouse "github.com/elaunira/openbao-plugin-database-clickhouse"
	"github.com/openbao/openbao/sdk/v2/database/dbplugin/v5"
)

var (
	version = "dev"
)

func main() {
	Run()
}

// Run instantiates a clickhouse object and runs the RPC server for the plugin.
func Run() {
	f := clickhouse.New(clickhouse.DefaultUserNameTemplate(), version)

	dbplugin.ServeMultiplex(f)
}
