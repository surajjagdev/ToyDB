package wal

import "time"

// Helps track metrics of wal appends and other operations

type Metrics interface {
	ObserveAppendBatch(n int)
	ObserveFsyncLatency(d time.Duration)
	ObserveAppendLatency(d time.Duration)
	IncFsyncError()
	IncRotation()
	AddBytesWritten(n int64)
}

// Empty base struct
type NopMetrics struct{}

func (NopMetrics) ObserveAppendBatch(int)             {}
func (NopMetrics) ObserveFsyncLatency(time.Duration)  {}
func (NopMetrics) ObserveAppendLatency(time.Duration) {}
func (NopMetrics) IncFsyncError()                     {}
func (NopMetrics) IncRotation()                       {}
func (NopMetrics) AddBytesWritten(int64)              {}
