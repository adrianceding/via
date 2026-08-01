package config

import (
	"errors"
	"io"
	"os"
	"syscall"
)

var (
	ErrConfigOpen     = errors.New("config: open failed")
	ErrConfigFile     = errors.New("config: unsafe file")
	ErrConfigTooLarge = errors.New("config: file too large")
	ErrConfigRead     = errors.New("config: read failed")
)

func LoadClient(path string) (Client, error) {
	data, err := loadConfigFile(path)
	if err != nil {
		return Client{}, err
	}
	return DecodeClient(data)
}

func LoadServer(path string) (Server, error) {
	data, err := loadConfigFile(path)
	if err != nil {
		return Server{}, err
	}
	return DecodeServer(data)
}

func loadConfigFile(path string) ([]byte, error) {
	// O_NONBLOCK prevents pipes or devices from blocking before the regular-file check on the same descriptor.
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrConfigOpen
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, ErrConfigFile
	}
	if !info.Mode().IsRegular() {
		return nil, ErrConfigFile
	}
	if info.Size() > MaxConfigBytes {
		return nil, ErrConfigTooLarge
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxConfigBytes+1))
	if err != nil {
		return nil, ErrConfigRead
	}
	if len(data) > MaxConfigBytes {
		return nil, ErrConfigTooLarge
	}
	return data, nil
}
