package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/adrianceding/via/internal/config"
)

var version = "dev"

type runners struct {
	client func(context.Context, config.Client) error
	server func(context.Context, config.Server) error
}

type contextFactory func() (context.Context, context.CancelFunc)

var productionRunners = runners{
	client: runClient,
	server: runServer,
}

func main() {
	if err := executeWithContextFactory(os.Args[1:], os.Stdout, os.Stderr, productionRunners, func() (context.Context, context.CancelFunc) {
		return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func execute(ctx context.Context, arguments []string, stdout, stderr io.Writer, roleRunners runners) error {
	if ctx == nil {
		return errors.New("via: invalid internal startup configuration")
	}
	return executeWithContextFactory(arguments, stdout, stderr, roleRunners, func() (context.Context, context.CancelFunc) {
		return ctx, func() {}
	})
}

func executeWithContextFactory(arguments []string, stdout, stderr io.Writer, roleRunners runners, newContext contextFactory) error {
	if stdout == nil || stderr == nil || roleRunners.client == nil || roleRunners.server == nil || newContext == nil {
		return errors.New("via: invalid internal startup configuration")
	}
	if len(arguments) == 1 && (arguments[0] == "version" || arguments[0] == "--version" || arguments[0] == "-version") {
		_, err := fmt.Fprintf(stdout, "via %s\n", version)
		return err
	}
	if len(arguments) == 0 {
		return usageError()
	}
	var start func(context.Context) error
	switch arguments[0] {
	case "client":
		path, err := parseConfigFlag("client", arguments[1:], stderr)
		if err != nil {
			return err
		}
		configuration, err := config.LoadClient(path)
		if err != nil {
			return err
		}
		start = func(ctx context.Context) error { return roleRunners.client(ctx, configuration) }
	case "server":
		path, err := parseConfigFlag("server", arguments[1:], stderr)
		if err != nil {
			return err
		}
		configuration, err := config.LoadServer(path)
		if err != nil {
			return err
		}
		start = func(ctx context.Context) error { return roleRunners.server(ctx, configuration) }
	default:
		return usageError()
	}

	ctx, stop := newContext()
	if stop != nil {
		defer stop()
	}
	if ctx == nil || stop == nil {
		return errors.New("via: invalid internal startup configuration")
	}
	return start(ctx)
}

func parseConfigFlag(role string, arguments []string, stderr io.Writer) (string, error) {
	flags := flag.NewFlagSet(role, flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "", "YAML configuration file")
	if err := flags.Parse(arguments); err != nil || *path == "" || flags.NArg() != 0 {
		return "", usageError()
	}
	return *path, nil
}

func usageError() error {
	return errors.New("usage: via client --config PATH | via server --config PATH | via version")
}
