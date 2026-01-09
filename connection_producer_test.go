// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_connStringBuilder_BuildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		builder  *ConnStringBuilder
		expected string
	}{
		{
			name: "basic connection",
			builder: newConnStringBuilder().
				WithHost("localhost").
				WithPort(9000),
			expected: "clickhouse://localhost:9000",
		},
		{
			name: "with database",
			builder: newConnStringBuilder().
				WithHost("localhost").
				WithPort(9000).
				WithDatabase("testdb"),
			expected: "clickhouse://localhost:9000/testdb",
		},
		{
			name: "with credentials",
			builder: newConnStringBuilder().
				WithHost("localhost").
				WithPort(9000).
				WithUsername("user").
				WithPassword("pass"),
			expected: "clickhouse://localhost:9000?password=pass&username=user",
		},
		{
			name: "with TLS",
			builder: newConnStringBuilder().
				WithHost("localhost").
				WithPort(9440).
				WithTLS(true, false),
			expected: "clickhouse://localhost:9440?secure=true",
		},
		{
			name: "with TLS skip verify",
			builder: newConnStringBuilder().
				WithHost("localhost").
				WithPort(9440).
				WithTLS(true, true),
			expected: "clickhouse://localhost:9440?secure=true&skip_verify=true",
		},
		{
			name: "with debug",
			builder: newConnStringBuilder().
				WithHost("localhost").
				WithPort(9000).
				WithDebug(true),
			expected: "clickhouse://localhost:9000?debug=true",
		},
		{
			name: "full configuration",
			builder: newConnStringBuilder().
				WithHost("clickhouse.example.com").
				WithPort(9440).
				WithDatabase("mydb").
				WithUsername("admin").
				WithPassword("secret").
				WithTLS(true, true).
				WithDebug(true),
			expected: "clickhouse://clickhouse.example.com:9440/mydb?debug=true&password=secret&secure=true&skip_verify=true&username=admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.builder.BuildConnectionString()
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_connStringBuilder_Check(t *testing.T) {
	tests := []struct {
		name      string
		builder   *ConnStringBuilder
		expectErr bool
	}{
		{
			name: "valid configuration",
			builder: newConnStringBuilder().
				WithHost("localhost").
				WithPort(9000),
			expectErr: false,
		},
		{
			name: "missing host",
			builder: newConnStringBuilder().
				WithPort(9000),
			expectErr: true,
		},
		{
			name: "missing port",
			builder: newConnStringBuilder().
				WithHost("localhost"),
			expectErr: true,
		},
		{
			name:      "empty configuration",
			builder:   newConnStringBuilder(),
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.builder.Check()
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewConnStringBuilderFromConnString(t *testing.T) {
	tests := []struct {
		name       string
		connString string
		expectHost string
		expectPort int
		expectDB   string
		expectTLS  bool
		expectErr  bool
	}{
		{
			name:       "basic connection string",
			connString: "clickhouse://localhost:9000",
			expectHost: "localhost",
			expectPort: 9000,
			expectErr:  false,
		},
		{
			name:       "with database",
			connString: "clickhouse://localhost:9000/testdb",
			expectHost: "localhost",
			expectPort: 9000,
			expectDB:   "testdb",
			expectErr:  false,
		},
		{
			name:       "with TLS",
			connString: "clickhouse://localhost:9440?secure=true",
			expectHost: "localhost",
			expectPort: 9440,
			expectTLS:  true,
			expectErr:  false,
		},
		{
			name:       "tcp scheme",
			connString: "tcp://localhost:9000",
			expectHost: "localhost",
			expectPort: 9000,
			expectErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, err := NewConnStringBuilderFromConnString(tt.connString)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expectHost, builder.host)
			require.Equal(t, tt.expectPort, builder.port)
			require.Equal(t, tt.expectDB, builder.database)
			require.Equal(t, tt.expectTLS, builder.tls)
		})
	}
}

func Test_connStringBuilder_WithExtraParam(t *testing.T) {
	builder := newConnStringBuilder().
		WithHost("localhost").
		WithPort(9000).
		WithExtraParam("dial_timeout", "10s").
		WithExtraParam("read_timeout", "30s")

	result := builder.BuildConnectionString()
	require.Contains(t, result, "dial_timeout=10s")
	require.Contains(t, result, "read_timeout=30s")
}
