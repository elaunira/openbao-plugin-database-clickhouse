// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver
	"github.com/mitchellh/mapstructure"
)

// clickhouseConnectionProducer implements the database.ConnectionProducer interface.
type clickhouseConnectionProducer struct {
	ConnectionURL          string `json:"connection_url" mapstructure:"connection_url"`
	Host                   string `json:"host" mapstructure:"host"`
	Port                   int    `json:"port" mapstructure:"port"`
	Username               string `json:"username" mapstructure:"username"`
	Password               string `json:"password" mapstructure:"password"`
	Database               string `json:"database" mapstructure:"database"`
	TLS                    bool   `json:"tls" mapstructure:"tls"`
	TLSSkipVerify          bool   `json:"tls_skip_verify" mapstructure:"tls_skip_verify"`
	MaxOpenConnections     int    `json:"max_open_connections" mapstructure:"max_open_connections"`
	MaxIdleConnections     int    `json:"max_idle_connections" mapstructure:"max_idle_connections"`
	MaxConnectionLifetimeS int    `json:"max_connection_lifetime" mapstructure:"max_connection_lifetime"`
	Debug                  bool   `json:"debug" mapstructure:"debug"`

	initialized bool
	db          *sql.DB
	sync.Mutex
}

// Init initializes the connection producer with the provided configuration.
func (c *clickhouseConnectionProducer) Init(ctx context.Context, conf map[string]interface{}, verifyConnection bool) error {
	c.Lock()
	defer c.Unlock()

	if err := mapstructure.WeakDecode(conf, c); err != nil {
		return fmt.Errorf("failed to decode configuration: %w", err)
	}

	// Set defaults
	if c.MaxOpenConnections == 0 {
		c.MaxOpenConnections = 4
	}
	if c.MaxIdleConnections == 0 {
		c.MaxIdleConnections = c.MaxOpenConnections
	}
	if c.MaxConnectionLifetimeS == 0 {
		c.MaxConnectionLifetimeS = 0 // No limit
	}

	// Build connection URL if not provided
	if c.ConnectionURL == "" {
		builder := newConnStringBuilder().
			WithHost(c.Host).
			WithPort(c.Port).
			WithDatabase(c.Database).
			WithUsername(c.Username).
			WithPassword(c.Password).
			WithTLS(c.TLS, c.TLSSkipVerify).
			WithDebug(c.Debug)

		if err := builder.Check(); err != nil {
			return fmt.Errorf("invalid connection configuration: %w", err)
		}

		c.ConnectionURL = builder.BuildConnectionString()
	} else {
		// Substitute {{username}} and {{password}} placeholders in connection URL
		// URL-encode the values to handle special characters
		c.ConnectionURL = strings.ReplaceAll(c.ConnectionURL, "{{username}}", url.PathEscape(c.Username))
		c.ConnectionURL = strings.ReplaceAll(c.ConnectionURL, "{{password}}", url.PathEscape(c.Password))
	}

	c.initialized = true

	if verifyConnection {
		db, err := c.Connection(ctx)
		if err != nil {
			return fmt.Errorf("failed to verify connection: %w", err)
		}
		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("failed to ping database: %w", err)
		}
	}

	return nil
}

// Connection returns a database connection.
func (c *clickhouseConnectionProducer) Connection(ctx context.Context) (*sql.DB, error) {
	if !c.initialized {
		return nil, fmt.Errorf("connection producer not initialized")
	}

	if c.db != nil {
		if err := c.db.PingContext(ctx); err == nil {
			return c.db, nil
		}
		// Connection is stale, close it
		_ = c.db.Close()
		c.db = nil
	}

	db, err := sql.Open("clickhouse", c.ConnectionURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	db.SetMaxOpenConns(c.MaxOpenConnections)
	db.SetMaxIdleConns(c.MaxIdleConnections)
	if c.MaxConnectionLifetimeS > 0 {
		db.SetConnMaxLifetime(time.Duration(c.MaxConnectionLifetimeS) * time.Second)
	}

	c.db = db
	return db, nil
}

// Close closes the database connection.
func (c *clickhouseConnectionProducer) Close() error {
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

// SecretValues returns sensitive values for masking in logs.
func (c *clickhouseConnectionProducer) SecretValues() map[string]string {
	return map[string]string{
		c.Password: "[password]",
	}
}

const trueVal = "true"

// ConnStringBuilder is a builder for ClickHouse connection strings.
type ConnStringBuilder struct {
	host          string
	port          int
	database      string
	username      string
	password      string
	tls           bool
	tlsSkipVerify bool
	debug         bool
	extraParams   map[string]string
}

// newConnStringBuilder creates a new connection string builder.
func newConnStringBuilder() *ConnStringBuilder {
	return &ConnStringBuilder{
		extraParams: make(map[string]string),
	}
}

// NewConnStringBuilderFromConnString parses an existing connection string.
func NewConnStringBuilderFromConnString(connString string) (*ConnStringBuilder, error) {
	builder := newConnStringBuilder()

	u, err := url.Parse(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	builder.host = u.Hostname()

	if portStr := u.Port(); portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port: %w", err)
		}
		builder.port = port
	}

	builder.database = strings.TrimPrefix(u.Path, "/")

	if u.User != nil {
		builder.username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			builder.password = password
		}
	}

	q := u.Query()

	// Parse username/password from query if not in URL
	if builder.username == "" {
		builder.username = q.Get("username")
	}
	if builder.password == "" {
		builder.password = q.Get("password")
	}

	// Parse TLS settings
	if q.Get("secure") == trueVal {
		builder.tls = true
	}
	if q.Get("skip_verify") == trueVal {
		builder.tlsSkipVerify = true
	}

	// Parse debug
	if q.Get("debug") == trueVal {
		builder.debug = true
	}

	return builder, nil
}

// WithHost sets the host.
func (b *ConnStringBuilder) WithHost(host string) *ConnStringBuilder {
	b.host = host
	return b
}

// WithPort sets the port.
func (b *ConnStringBuilder) WithPort(port int) *ConnStringBuilder {
	b.port = port
	return b
}

// WithDatabase sets the database name.
func (b *ConnStringBuilder) WithDatabase(database string) *ConnStringBuilder {
	b.database = database
	return b
}

// WithUsername sets the username.
func (b *ConnStringBuilder) WithUsername(username string) *ConnStringBuilder {
	b.username = username
	return b
}

// WithPassword sets the password.
func (b *ConnStringBuilder) WithPassword(password string) *ConnStringBuilder {
	b.password = password
	return b
}

// WithTLS sets TLS configuration.
func (b *ConnStringBuilder) WithTLS(tls, skipVerify bool) *ConnStringBuilder {
	b.tls = tls
	b.tlsSkipVerify = skipVerify
	return b
}

// WithDebug sets debug mode.
func (b *ConnStringBuilder) WithDebug(debug bool) *ConnStringBuilder {
	b.debug = debug
	return b
}

// WithExtraParam adds an extra query parameter.
func (b *ConnStringBuilder) WithExtraParam(key, value string) *ConnStringBuilder {
	b.extraParams[key] = value
	return b
}

// Check validates the connection string builder configuration.
func (b *ConnStringBuilder) Check() error {
	if b.host == "" {
		return fmt.Errorf("host is required")
	}
	if b.port == 0 {
		return fmt.Errorf("port is required")
	}
	return nil
}

// BuildConnectionString builds a ClickHouse connection string.
func (b *ConnStringBuilder) BuildConnectionString() string {
	q := make(url.Values)

	if b.username != "" {
		q.Set("username", b.username)
	}
	if b.password != "" {
		q.Set("password", b.password)
	}
	if b.tls {
		q.Set("secure", trueVal)
		if b.tlsSkipVerify {
			q.Set("skip_verify", trueVal)
		}
	}
	if b.debug {
		q.Set("debug", trueVal)
	}

	for k, v := range b.extraParams {
		q.Set(k, v)
	}

	u := &url.URL{
		Scheme:   "clickhouse",
		Host:     fmt.Sprintf("%s:%d", b.host, b.port),
		Path:     b.database,
		RawQuery: q.Encode(),
	}

	return u.String()
}
