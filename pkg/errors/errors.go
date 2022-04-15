package errors

import "errors"

// Data Errors

var (
	ErrCheckStorageTipFailed = errors.New("unable to check storage tip")
	ErrDataCheckHalt         = errors.New("data check halted")
	ErrInitDataTester        = errors.New("unexpected error occurred while trying to initialize data tester")
)
