// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

package clickhouse

import (
	"context"
	"database/sql"
	"net/url"
	"regexp"
	"strconv"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	clickhousehelper "github.com/elaunira/openbao-plugin-database-clickhouse/testhelpers/clickhouse"
	"github.com/openbao/openbao/sdk/v2/database/dbplugin/v5"
	"github.com/stretchr/testify/require"
)

const (
	testAdminUser     = "default"
	testAdminPassword = "password"
	testRole          = "testrole"
	testPassword      = "testpassword123"
)

func TestClickhouse_Initialize(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	parsed, err := url.Parse(connURL)
	require.NoError(t, err)

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err = db.Initialize(context.Background(), req)
	require.NoError(t, err)

	_, ok := db.(dbplugin.DatabaseErrorSanitizerMiddleware)
	require.True(t, ok, "expected db to be DatabaseErrorSanitizerMiddleware")

	t.Logf("Connected to ClickHouse at %s", parsed.Host)
}

func TestClickhouse_Initialize_WithHostPort(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	parsed, err := url.Parse(connURL)
	require.NoError(t, err)

	port, err := strconv.Atoi(parsed.Port())
	require.NoError(t, err)

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"host":     parsed.Hostname(),
			"port":     port,
			"username": testAdminUser,
			"password": testAdminPassword,
		},
		VerifyConnection: true,
	}

	_, err = db.Initialize(context.Background(), req)
	require.NoError(t, err)
}

func TestClickhouse_NewUser(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	password := testPassword
	expiration := time.Now().Add(time.Hour)

	newUserReq := dbplugin.NewUserRequest{
		UsernameConfig: dbplugin.UsernameMetadata{
			DisplayName: "token",
			RoleName:    testRole,
		},
		Statements: dbplugin.Statements{
			Commands: []string{
				"CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'",
			},
		},
		Password:   password,
		Expiration: expiration,
	}

	resp, err := db.NewUser(context.Background(), newUserReq)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Username)

	// Verify username format
	matched, err := regexp.MatchString(`^v-token-testrole-[a-zA-Z0-9]{15,}`, resp.Username)
	require.NoError(t, err)
	require.True(t, matched, "username %q doesn't match expected pattern", resp.Username)

	// Verify the user can connect
	testConnURL := buildTestConnURL(connURL, resp.Username, password)
	err = clickhousehelper.TestCredsExist(t, testConnURL)
	require.NoError(t, err)

	t.Logf("Created user: %s", resp.Username)
}

func TestClickhouse_DeleteUser(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	password := testPassword
	expiration := time.Now().Add(time.Hour)

	// Create a user first
	newUserReq := dbplugin.NewUserRequest{
		UsernameConfig: dbplugin.UsernameMetadata{
			DisplayName: "token",
			RoleName:    testRole,
		},
		Statements: dbplugin.Statements{
			Commands: []string{
				"CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'",
			},
		},
		Password:   password,
		Expiration: expiration,
	}

	resp, err := db.NewUser(context.Background(), newUserReq)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Username)

	// Verify the user can connect
	testConnURL := buildTestConnURL(connURL, resp.Username, password)
	err = clickhousehelper.TestCredsExist(t, testConnURL)
	require.NoError(t, err)

	// Delete the user
	deleteReq := dbplugin.DeleteUserRequest{
		Username: resp.Username,
	}

	_, err = db.DeleteUser(context.Background(), deleteReq)
	require.NoError(t, err)

	// Verify the user can no longer connect
	err = clickhousehelper.TestCredsExist(t, testConnURL)
	require.Error(t, err)

	t.Logf("Deleted user: %s", resp.Username)
}

func TestClickhouse_UpdateUser(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	password := testPassword
	expiration := time.Now().Add(time.Hour)

	// Create a user first
	newUserReq := dbplugin.NewUserRequest{
		UsernameConfig: dbplugin.UsernameMetadata{
			DisplayName: "token",
			RoleName:    testRole,
		},
		Statements: dbplugin.Statements{
			Commands: []string{
				"CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'",
			},
		},
		Password:   password,
		Expiration: expiration,
	}

	resp, err := db.NewUser(context.Background(), newUserReq)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Username)

	// Update the password
	newPassword := "newpassword456"
	updateReq := dbplugin.UpdateUserRequest{
		Username: resp.Username,
		Password: &dbplugin.ChangePassword{
			NewPassword: newPassword,
		},
	}

	_, err = db.UpdateUser(context.Background(), updateReq)
	require.NoError(t, err)

	// Verify old password no longer works
	oldConnURL := buildTestConnURL(connURL, resp.Username, password)
	err = clickhousehelper.TestCredsExist(t, oldConnURL)
	require.Error(t, err)

	// Verify new password works
	newConnURL := buildTestConnURL(connURL, resp.Username, newPassword)
	err = clickhousehelper.TestCredsExist(t, newConnURL)
	require.NoError(t, err)

	t.Logf("Updated password for user: %s", resp.Username)
}

func TestClickhouse_UpdateUser_NoChanges(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	// Try to update with no changes
	updateReq := dbplugin.UpdateUserRequest{
		Username: "testuser",
	}

	_, err = db.UpdateUser(context.Background(), updateReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no changes requested")
}

func TestNew(t *testing.T) {
	f := New(DefaultUserNameTemplate(), "1.0.0")
	db, err := f()
	require.NoError(t, err)
	require.NotNil(t, db)

	dbImpl, ok := db.(dbplugin.Database)
	require.True(t, ok)

	typeName, err := dbImpl.Type()
	require.NoError(t, err)
	require.Equal(t, "clickhouse", typeName)
}

func TestClickhouse_NewUser_WithRoleAssignment(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	// Create a role first
	adminDB, err := sql.Open("clickhouse", connURL)
	require.NoError(t, err)
	defer func() { _ = adminDB.Close() }()

	_, err = adminDB.ExecContext(context.Background(), "CREATE ROLE IF NOT EXISTS test_reader")
	require.NoError(t, err)

	password := testPassword
	expiration := time.Now().Add(time.Hour)

	newUserReq := dbplugin.NewUserRequest{
		UsernameConfig: dbplugin.UsernameMetadata{
			DisplayName: "token",
			RoleName:    testRole,
		},
		Statements: dbplugin.Statements{
			Commands: []string{
				"CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'",
				"GRANT test_reader TO '{{name}}'",
			},
		},
		Password:   password,
		Expiration: expiration,
	}

	resp, err := db.NewUser(context.Background(), newUserReq)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Username)

	t.Logf("Created user with role: %s", resp.Username)
}

func TestClickhouse_UpdateUser_WithExpiration(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	password := testPassword
	expiration := time.Now().Add(time.Hour)

	// Create a user first
	newUserReq := dbplugin.NewUserRequest{
		UsernameConfig: dbplugin.UsernameMetadata{
			DisplayName: "token",
			RoleName:    testRole,
		},
		Statements: dbplugin.Statements{
			Commands: []string{
				"CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'",
			},
		},
		Password:   password,
		Expiration: expiration,
	}

	resp, err := db.NewUser(context.Background(), newUserReq)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Username)

	// Update expiration with a custom statement
	newExpiration := time.Now().Add(2 * time.Hour)
	updateReq := dbplugin.UpdateUserRequest{
		Username: resp.Username,
		Expiration: &dbplugin.ChangeExpiration{
			NewExpiration: newExpiration,
			Statements: dbplugin.Statements{
				Commands: []string{
					"ALTER USER '{{name}}' VALID UNTIL '{{expiration}}'",
				},
			},
		},
	}

	_, err = db.UpdateUser(context.Background(), updateReq)
	require.NoError(t, err)

	t.Logf("Updated expiration for user: %s", resp.Username)
}

func TestClickhouse_UpdateUser_ExpirationNoStatements(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	password := testPassword
	expiration := time.Now().Add(time.Hour)

	// Create a user first
	newUserReq := dbplugin.NewUserRequest{
		UsernameConfig: dbplugin.UsernameMetadata{
			DisplayName: "token",
			RoleName:    testRole,
		},
		Statements: dbplugin.Statements{
			Commands: []string{
				"CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'",
			},
		},
		Password:   password,
		Expiration: expiration,
	}

	resp, err := db.NewUser(context.Background(), newUserReq)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Username)

	// Update expiration without statements (should be a no-op)
	newExpiration := time.Now().Add(2 * time.Hour)
	updateReq := dbplugin.UpdateUserRequest{
		Username: resp.Username,
		Expiration: &dbplugin.ChangeExpiration{
			NewExpiration: newExpiration,
		},
	}

	_, err = db.UpdateUser(context.Background(), updateReq)
	require.NoError(t, err)

	t.Logf("Expiration update with no statements succeeded for user: %s", resp.Username)
}

func TestClickhouse_Initialize_TLS(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, true, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	t.Logf("Connected to ClickHouse with TLS")
}

func TestClickhouse_Type(t *testing.T) {
	f := New(DefaultUserNameTemplate(), "1.2.3")
	db, err := f()
	require.NoError(t, err)

	clickhouseDB := db.(dbplugin.Database)
	typeName, err := clickhouseDB.Type()
	require.NoError(t, err)
	require.Equal(t, "clickhouse", typeName)
}

func TestClickhouse_Close(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	// Close the database
	err = db.Close()
	require.NoError(t, err)

	// Closing again should not error
	err = db.Close()
	require.NoError(t, err)
}

func TestClickhouse_NewUser_MultipleStatements(t *testing.T) {
	cleanup, connURL := clickhousehelper.PrepareTestContainer(t, false, testAdminUser, testAdminPassword)
	defer cleanup()

	db := newTestDB(testAdminUser, testAdminPassword)

	req := dbplugin.InitializeRequest{
		Config: map[string]interface{}{
			"connection_url": connURL,
		},
		VerifyConnection: true,
	}

	_, err := db.Initialize(context.Background(), req)
	require.NoError(t, err)

	// Create a role first
	adminDB, err := sql.Open("clickhouse", connURL)
	require.NoError(t, err)
	defer func() { _ = adminDB.Close() }()

	_, err = adminDB.ExecContext(context.Background(), "CREATE ROLE IF NOT EXISTS multi_test_role")
	require.NoError(t, err)

	password := testPassword
	expiration := time.Now().Add(time.Hour)

	// Test multiple statements in a single command (semicolon separated)
	newUserReq := dbplugin.NewUserRequest{
		UsernameConfig: dbplugin.UsernameMetadata{
			DisplayName: "token",
			RoleName:    testRole,
		},
		Statements: dbplugin.Statements{
			Commands: []string{
				"CREATE USER IF NOT EXISTS '{{name}}' IDENTIFIED BY '{{password}}'; GRANT multi_test_role TO '{{name}}'",
			},
		},
		Password:   password,
		Expiration: expiration,
	}

	resp, err := db.NewUser(context.Background(), newUserReq)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Username)

	t.Logf("Created user with multiple statements: %s", resp.Username)
}

func Test_splitStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single statement",
			input:    "CREATE USER 'test'",
			expected: []string{"CREATE USER 'test'"},
		},
		{
			name:     "multiple statements",
			input:    "CREATE USER 'test'; GRANT role TO 'test'",
			expected: []string{"CREATE USER 'test'", "GRANT role TO 'test'"},
		},
		{
			name:     "semicolon in single quotes",
			input:    "CREATE USER 'test;user' IDENTIFIED BY 'pass;word'",
			expected: []string{"CREATE USER 'test;user' IDENTIFIED BY 'pass;word'"},
		},
		{
			name:     "semicolon in double quotes",
			input:    `CREATE USER "test;user" IDENTIFIED BY "pass;word"`,
			expected: []string{`CREATE USER "test;user" IDENTIFIED BY "pass;word"`},
		},
		{
			name:     "mixed quotes with semicolons",
			input:    `CREATE USER 'test;user'; GRANT "role;name" TO 'test;user'`,
			expected: []string{`CREATE USER 'test;user'`, `GRANT "role;name" TO 'test;user'`},
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
		},
		{
			name:     "only whitespace",
			input:    "   ",
			expected: nil,
		},
		{
			name:     "trailing semicolon",
			input:    "CREATE USER 'test';",
			expected: []string{"CREATE USER 'test'"},
		},
		{
			name:     "multiple semicolons",
			input:    "SELECT 1;; SELECT 2",
			expected: []string{"SELECT 1", "SELECT 2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitStatements(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func newTestDB(_, _ string) dbplugin.Database {
	f := New(DefaultUserNameTemplate(), "test")
	db, _ := f()
	return db.(dbplugin.Database)
}

func buildTestConnURL(baseURL, username, password string) string {
	parsed, _ := url.Parse(baseURL)
	q := parsed.Query()
	q.Set("username", username)
	q.Set("password", password)
	parsed.RawQuery = q.Encode()
	return parsed.String()
}
