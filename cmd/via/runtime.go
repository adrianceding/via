package main

import (
	"context"

	"github.com/adrianceding/via/internal/config"
	"github.com/adrianceding/via/internal/daemon"
)

func runClient(ctx context.Context, configuration config.Client) error {
	return daemon.RunClient(ctx, configuration)
}

func runServer(ctx context.Context, configuration config.Server) error {
	return daemon.RunServer(ctx, configuration)
}
