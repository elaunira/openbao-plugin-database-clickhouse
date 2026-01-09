// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

package clickhousehelper

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"testing"

	"github.com/openbao/openbao/sdk/v2/helper/docker"
)

// Config holds the ClickHouse connection configuration.
type Config struct {
	docker.ServiceHostPort
	ConnString string
}

var _ docker.ServiceConfig = &Config{}

// PrepareTestContainer starts a ClickHouse container for testing.
func PrepareTestContainer(t *testing.T, useTLS bool, adminUser, adminPassword string) (func(), string) {
	if os.Getenv("CLICKHOUSE_URL") != "" {
		return func() {}, os.Getenv("CLICKHOUSE_URL")
	}

	imageVersion := "24.8-alpine"
	extraCopy := map[string]string{}
	ports := []string{"9000/tcp"}

	if useTLS {
		if err := genCACertificates("testhelpers/resources/certs"); err != nil {
			t.Fatalf("unable to generate SSL Certificates: %v", err)
		}
		extraCopy["testhelpers/resources/certs"] = "/etc/clickhouse-server/certs"
		extraCopy["testhelpers/resources/config.xml"] = "/etc/clickhouse-server/config.xml"
		ports = []string{"9440/tcp"}
	}

	runner, err := docker.NewServiceRunner(docker.RunOptions{
		ImageRepo:     "clickhouse/clickhouse-server",
		ImageTag:      imageVersion,
		ContainerName: "clickhouse-server",
		Env: []string{
			"CLICKHOUSE_USER=" + adminUser,
			"CLICKHOUSE_PASSWORD=" + adminPassword,
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		},
		CopyFromTo:      extraCopy,
		Ports:           ports,
		DoNotAutoRemove: false,
	})
	if err != nil {
		t.Fatalf("could not start docker clickhouse: %s", err)
	}

	svc, err := runner.StartService(context.Background(), func(ctx context.Context, host string, port int) (docker.ServiceConfig, error) {
		hostIP := docker.NewServiceHostPort(host, port)
		q := make(url.Values)
		q.Set("username", adminUser)
		q.Set("password", adminPassword)

		if useTLS {
			q.Set("secure", "true")
			q.Set("skip_verify", "true")
		}

		dsn := (&url.URL{
			Scheme:   "clickhouse",
			Host:     hostIP.Address(),
			RawQuery: q.Encode(),
		}).String()

		db, err := sql.Open("clickhouse", dsn)
		if err != nil {
			return nil, err
		}
		defer db.Close()

		err = db.Ping()
		if err != nil {
			return nil, err
		}

		return &Config{ServiceHostPort: *hostIP, ConnString: dsn}, nil
	})
	if err != nil {
		t.Fatalf("could not start docker clickhouse: %s", err)
	}

	return svc.Cleanup, svc.Config.(*Config).ConnString
}

// TestCredsExist tests if the provided credentials can connect to ClickHouse.
func TestCredsExist(t testing.TB, connURL string) error {
	db, err := sql.Open("clickhouse", connURL)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Ping()
}

// BuildConnString builds a connection string from components.
func BuildConnString(host string, port int, username, password string, useTLS, skipVerify bool) string {
	q := make(url.Values)
	q.Set("username", username)
	q.Set("password", password)

	if useTLS {
		q.Set("secure", "true")
		if skipVerify {
			q.Set("skip_verify", "true")
		}
	}

	return (&url.URL{
		Scheme:   "clickhouse",
		Host:     host + ":" + string(rune(port)),
		RawQuery: q.Encode(),
	}).String()
}
