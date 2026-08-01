package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adrianceding/via/internal/config"
)

func TestExecuteLoadsStrictYAMLBeforeCallingRole(t *testing.T) {
	directory := t.TempDir()
	clientPath := writeCLIConfig(t, directory, "client.yml", `socks_listen: "127.0.0.1:1080"
socks_auth: {username: local-user, password: local-password}
transport: {type: tcp, address: "127.0.0.1:9443"}
principal_id: client-01
psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
`)
	serverPath := writeCLIConfig(t, directory, "server.yml", `transport: {type: tcp, listen: "127.0.0.1:9443"}
principals:
  - id: client-01
    psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
`)
	clientCalls, serverCalls := 0, 0
	roleRunners := runners{
		client: func(_ context.Context, configuration config.Client) error {
			clientCalls++
			if configuration.PrincipalID != "client-01" || configuration.RequiredBytes != config.DefaultClientRequiredBytes {
				t.Fatalf("client configuration = %#v", configuration)
			}
			return nil
		},
		server: func(_ context.Context, configuration config.Server) error {
			serverCalls++
			if len(configuration.Principals) != 1 || configuration.RequiredBytes != config.DefaultServerRequiredBytes {
				t.Fatalf("server configuration = %#v", configuration)
			}
			return nil
		},
	}
	for _, arguments := range [][]string{{"client", "--config", clientPath}, {"server", "--config", serverPath}} {
		if err := execute(context.Background(), arguments, &bytes.Buffer{}, &bytes.Buffer{}, roleRunners); err != nil {
			t.Fatal(err)
		}
	}
	if clientCalls != 1 || serverCalls != 1 {
		t.Fatalf("role calls = client %d server %d", clientCalls, serverCalls)
	}
}

func TestExecuteRejectsInvalidArgumentsAndConfigurationBeforeRunner(t *testing.T) {
	calls := 0
	roleRunners := runners{
		client: func(context.Context, config.Client) error { calls++; return nil },
		server: func(context.Context, config.Server) error { calls++; return nil },
	}
	invalidArguments := [][]string{nil, {"unknown"}, {"client"}, {"server", "--config"}, {"client", "--config", "a", "extra"}}
	for _, arguments := range invalidArguments {
		if err := execute(context.Background(), arguments, &bytes.Buffer{}, &bytes.Buffer{}, roleRunners); err == nil {
			t.Fatalf("arguments %#v accepted", arguments)
		}
	}
	directory := t.TempDir()
	invalid := writeCLIConfig(t, directory, "invalid.yml", "not: a-client\n")
	if err := execute(context.Background(), []string{"client", "--config", invalid}, &bytes.Buffer{}, &bytes.Buffer{}, roleRunners); err == nil {
		t.Fatal("invalid configuration accepted")
	}
	if calls != 0 {
		t.Fatalf("runner called %d times", calls)
	}
}

func TestExecuteCreatesLifecycleContextOnlyAfterConfigurationValidation(t *testing.T) {
	directory := t.TempDir()
	valid := writeCLIConfig(t, directory, "client.yml", `socks_listen: "127.0.0.1:1080"
socks_auth: {username: local-user, password: local-password}
transport: {type: tcp, address: "127.0.0.1:9443"}
principal_id: client-01
psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
`)
	invalid := writeCLIConfig(t, directory, "invalid.yml", "not: a-client\n")
	contextCalls := 0
	stops := 0
	runnerCalls := 0
	newContext := func() (context.Context, context.CancelFunc) {
		contextCalls++
		return context.Background(), func() { stops++ }
	}
	roleRunners := runners{
		client: func(context.Context, config.Client) error { runnerCalls++; return nil },
		server: func(context.Context, config.Server) error { runnerCalls++; return nil },
	}

	for _, arguments := range [][]string{nil, {"version"}, {"client", "--config", invalid}} {
		if err := executeWithContextFactory(arguments, &bytes.Buffer{}, &bytes.Buffer{}, roleRunners, newContext); arguments != nil && arguments[0] == "version" {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil {
			t.Fatalf("invalid arguments %#v accepted", arguments)
		}
	}
	if contextCalls != 0 || stops != 0 || runnerCalls != 0 {
		t.Fatalf("pre-validation lifecycle = contexts %d stops %d runners %d", contextCalls, stops, runnerCalls)
	}
	if err := executeWithContextFactory([]string{"client", "--config", valid}, &bytes.Buffer{}, &bytes.Buffer{}, roleRunners, newContext); err != nil {
		t.Fatal(err)
	}
	if contextCalls != 1 || stops != 1 || runnerCalls != 1 {
		t.Fatalf("valid lifecycle = contexts %d stops %d runners %d", contextCalls, stops, runnerCalls)
	}
}

func TestExecuteVersion(t *testing.T) {
	stdout := &bytes.Buffer{}
	roleRunners := runners{
		client: func(context.Context, config.Client) error { return nil },
		server: func(context.Context, config.Server) error { return nil },
	}
	if err := execute(context.Background(), []string{"version"}, stdout, &bytes.Buffer{}, roleRunners); err != nil || !strings.Contains(stdout.String(), version) {
		t.Fatalf("version = %q, %v", stdout.String(), err)
	}
}

func TestExecutePropagatesRunnerFailure(t *testing.T) {
	directory := t.TempDir()
	path := writeCLIConfig(t, directory, "client.yml", `socks_listen: "127.0.0.1:1080"
socks_auth: {username: local-user, password: local-password}
transport: {type: tcp, address: "127.0.0.1:9443"}
principal_id: client-01
psk: "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="
`)
	want := errors.New("runner failed")
	err := execute(context.Background(), []string{"client", "--config", path}, &bytes.Buffer{}, &bytes.Buffer{}, runners{
		client: func(context.Context, config.Client) error { return want },
		server: func(context.Context, config.Server) error { return nil },
	})
	if !errors.Is(err, want) {
		t.Fatalf("runner error = %v", err)
	}
}

func writeCLIConfig(t *testing.T, directory, name, content string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
