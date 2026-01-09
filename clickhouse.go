// Copyright (c) 2024 Elaunira
// SPDX-License-Identifier: MPL-2.0

package clickhouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-secure-stdlib/strutil"
	"github.com/openbao/openbao/sdk/v2/database/dbplugin/v5"
	"github.com/openbao/openbao/sdk/v2/database/helper/dbutil"
	"github.com/openbao/openbao/sdk/v2/helper/template"
	"github.com/openbao/openbao/sdk/v2/logical"
)

const (
	clickhouseTypeName = "clickhouse"

	defaultUserNameTemplate = `{{ printf "v-%s-%s-%s-%s" (.DisplayName | truncate 8) (.RoleName | truncate 8) (random 15) (unix_time) | truncate 32 }}`

	defaultRevocationStatement        = `DROP USER IF EXISTS '{{name}}'`
	defaultRotateCredentialsStatement = `ALTER USER IF EXISTS '{{name}}' IDENTIFIED BY '{{password}}'`
)

var _ dbplugin.Database = (*Clickhouse)(nil)

// UsernameMetadata holds the metadata used for username generation.
type UsernameMetadata struct {
	DisplayName string
	RoleName    string
}

// Clickhouse is the database plugin implementation for ClickHouse.
type Clickhouse struct {
	*clickhouseConnectionProducer
	usernameProducer template.StringTemplate
	version          string
}

// New returns a new Clickhouse instance with the provided username template and version.
func New(usernameTemplate, version string) func() (interface{}, error) {
	return func() (interface{}, error) {
		if usernameTemplate == "" {
			usernameTemplate = defaultUserNameTemplate
		}

		up, err := template.NewTemplate(template.Template(usernameTemplate))
		if err != nil {
			return nil, fmt.Errorf("failed to parse username template: %w", err)
		}

		db := &Clickhouse{
			clickhouseConnectionProducer: &clickhouseConnectionProducer{},
			usernameProducer:             up,
			version:                      version,
		}

		wrapped := dbplugin.NewDatabaseErrorSanitizerMiddleware(db, db.secretValues)

		return wrapped, nil
	}
}

// DefaultUserNameTemplate returns the default username template.
func DefaultUserNameTemplate() string {
	return defaultUserNameTemplate
}

// Type returns the type of the database plugin.
func (c *Clickhouse) Type() (string, error) {
	return clickhouseTypeName, nil
}

// Metadata returns the plugin metadata.
func (c *Clickhouse) Metadata() (map[string]interface{}, error) {
	return map[string]interface{}{
		"version": c.version,
		"type":    clickhouseTypeName,
	}, nil
}

// PluginVersion returns the version of the plugin.
func (c *Clickhouse) PluginVersion() logical.PluginVersion {
	return logical.PluginVersion{
		Version: c.version,
	}
}

// Initialize initializes the database plugin with the provided configuration.
func (c *Clickhouse) Initialize(ctx context.Context, req dbplugin.InitializeRequest) (dbplugin.InitializeResponse, error) {
	usernameTemplate, err := strutil.GetString(req.Config, "username_template")
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("failed to get username_template: %w", err)
	}

	if usernameTemplate == "" {
		usernameTemplate = defaultUserNameTemplate
	}

	up, err := template.NewTemplate(template.Template(usernameTemplate))
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("failed to parse username_template: %w", err)
	}
	c.usernameProducer = up

	err = c.clickhouseConnectionProducer.Init(ctx, req.Config, req.VerifyConnection)
	if err != nil {
		return dbplugin.InitializeResponse{}, fmt.Errorf("failed to initialize connection producer: %w", err)
	}

	resp := dbplugin.InitializeResponse{
		Config: req.Config,
	}

	return resp, nil
}

// NewUser creates a new user in the ClickHouse database.
func (c *Clickhouse) NewUser(ctx context.Context, req dbplugin.NewUserRequest) (dbplugin.NewUserResponse, error) {
	if len(req.Statements.Commands) == 0 {
		return dbplugin.NewUserResponse{}, fmt.Errorf("no creation statements provided")
	}

	c.Lock()
	defer c.Unlock()

	username, err := c.usernameProducer.Generate(UsernameMetadata{
		DisplayName: req.UsernameConfig.DisplayName,
		RoleName:    req.UsernameConfig.RoleName,
	})
	if err != nil {
		return dbplugin.NewUserResponse{}, fmt.Errorf("failed to generate username: %w", err)
	}

	expirationStr := req.Expiration.Format(time.DateTime)

	err = c.executeStatementsWithMap(ctx, req.Statements.Commands, map[string]string{
		"name":       username,
		"username":   username,
		"password":   req.Password,
		"expiration": expirationStr,
	})
	if err != nil {
		return dbplugin.NewUserResponse{}, fmt.Errorf("failed to create user: %w", err)
	}

	return dbplugin.NewUserResponse{
		Username: username,
	}, nil
}

// UpdateUser updates an existing user in the ClickHouse database.
func (c *Clickhouse) UpdateUser(ctx context.Context, req dbplugin.UpdateUserRequest) (dbplugin.UpdateUserResponse, error) {
	if req.Password == nil && req.Expiration == nil {
		return dbplugin.UpdateUserResponse{}, fmt.Errorf("no changes requested")
	}

	c.Lock()
	defer c.Unlock()

	if req.Password != nil {
		err := c.updateUserPassword(ctx, req.Username, req.Password)
		if err != nil {
			return dbplugin.UpdateUserResponse{}, err
		}
	}

	if req.Expiration != nil {
		err := c.updateUserExpiration(ctx, req.Username, req.Expiration)
		if err != nil {
			return dbplugin.UpdateUserResponse{}, err
		}
	}

	return dbplugin.UpdateUserResponse{}, nil
}

func (c *Clickhouse) updateUserPassword(ctx context.Context, username string, changePassword *dbplugin.ChangePassword) error {
	statements := changePassword.Statements.Commands
	if len(statements) == 0 {
		statements = []string{defaultRotateCredentialsStatement}
	}

	return c.executeStatementsWithMap(ctx, statements, map[string]string{
		"name":     username,
		"username": username,
		"password": changePassword.NewPassword,
	})
}

func (c *Clickhouse) updateUserExpiration(ctx context.Context, username string, changeExpiration *dbplugin.ChangeExpiration) error {
	statements := changeExpiration.Statements.Commands
	if len(statements) == 0 {
		// No expiration update statements, nothing to do
		return nil
	}

	expirationStr := changeExpiration.NewExpiration.Format(time.DateTime)

	return c.executeStatementsWithMap(ctx, statements, map[string]string{
		"name":       username,
		"username":   username,
		"expiration": expirationStr,
	})
}

// DeleteUser deletes a user from the ClickHouse database.
func (c *Clickhouse) DeleteUser(ctx context.Context, req dbplugin.DeleteUserRequest) (dbplugin.DeleteUserResponse, error) {
	c.Lock()
	defer c.Unlock()

	statements := req.Statements.Commands
	if len(statements) == 0 {
		statements = []string{defaultRevocationStatement}
	}

	err := c.executeStatementsWithMap(ctx, statements, map[string]string{
		"name":     req.Username,
		"username": req.Username,
	})
	if err != nil {
		return dbplugin.DeleteUserResponse{}, fmt.Errorf("failed to delete user: %w", err)
	}

	return dbplugin.DeleteUserResponse{}, nil
}

func (c *Clickhouse) executeStatementsWithMap(ctx context.Context, statements []string, m map[string]string) error {
	db, err := c.Connection(ctx)
	if err != nil {
		return err
	}

	for _, statement := range statements {
		parsedStatement := dbutil.QueryHelper(statement, m)

		// Split statements by semicolon for multiple statements
		for _, s := range splitStatements(parsedStatement) {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}

			_, err := db.ExecContext(ctx, s)
			if err != nil {
				return fmt.Errorf("failed to execute statement %q: %w", s, err)
			}
		}
	}

	return nil
}

func splitStatements(s string) []string {
	// Simple split by semicolon, but handle quoted strings
	var statements []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, char := range s {
		switch {
		case char == '\'' || char == '"':
			if !inQuote {
				inQuote = true
				quoteChar = char
			} else if char == quoteChar {
				inQuote = false
			}
			current.WriteRune(char)
		case char == ';' && !inQuote:
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		default:
			current.WriteRune(char)
		}
	}

	// Add the last statement if any
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

func (c *Clickhouse) secretValues() map[string]string {
	return map[string]string{
		c.Password: "[password]",
	}
}

// Lock locks the connection producer mutex.
func (c *Clickhouse) Lock() {
	c.clickhouseConnectionProducer.Lock()
}

// Unlock unlocks the connection producer mutex.
func (c *Clickhouse) Unlock() {
	c.clickhouseConnectionProducer.Unlock()
}

// Close closes the database connection.
func (c *Clickhouse) Close() error {
	c.Lock()
	defer c.Unlock()

	return c.clickhouseConnectionProducer.Close()
}

// clickhouseConnectionProducer mutex wrapper
type syncMutex struct {
	sync.Mutex
}

// ValidateUsername validates the username against a regex pattern.
func ValidateUsername(username, pattern string) bool {
	matched, _ := regexp.MatchString(pattern, username)
	return matched
}
