// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ego

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/log"
)

// spyLogger records the last method called, the message, and any structured
// fields passed to it. It implements Logger and is used to verify that
// loggerAdapter routes each GoAkt method to the correct inner method with the
// correct message and fields.
type spyLogger struct {
	lastMethod string
	lastMsg    string
	lastFields []any
}

func (s *spyLogger) Debug(msg string, args ...any) {
	s.lastMethod = "debug"
	s.lastMsg = msg
	s.lastFields = args
}
func (s *spyLogger) Info(msg string, args ...any) {
	s.lastMethod = "info"
	s.lastMsg = msg
	s.lastFields = args
}
func (s *spyLogger) Warn(msg string, args ...any) {
	s.lastMethod = "warn"
	s.lastMsg = msg
	s.lastFields = args
}
func (s *spyLogger) Error(msg string, args ...any) {
	s.lastMethod = "error"
	s.lastMsg = msg
	s.lastFields = args
}

func newAdapter(spy *spyLogger) *loggerAdapter {
	return newLoggerAdapter(spy)
}

// leveledSpyLogger extends spyLogger with LeveledLogger support.
type leveledSpyLogger struct {
	spyLogger
	level string
}

func (l *leveledSpyLogger) Level() string { return l.level }

// -----------------------------------------------------------------------------
// Debug family
// -----------------------------------------------------------------------------

func TestLoggerAdapterDebug(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Debug("debug msg")
	assert.Equal(t, "debug", spy.lastMethod)
	assert.Equal(t, "debug msg", spy.lastMsg)
}

func TestLoggerAdapterDebugf(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Debugf("hello %s", "world")
	assert.Equal(t, "debug", spy.lastMethod)
	assert.Equal(t, "hello world", spy.lastMsg)
}

func TestLoggerAdapterDebugContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.DebugContext(context.Background(), "ctx debug")
	assert.Equal(t, "debug", spy.lastMethod)
	assert.Equal(t, "ctx debug", spy.lastMsg)
}

func TestLoggerAdapterDebugfContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.DebugfContext(context.Background(), "ctx %s", "debugf")
	assert.Equal(t, "debug", spy.lastMethod)
	assert.Equal(t, "ctx debugf", spy.lastMsg)
}

// -----------------------------------------------------------------------------
// Info family
// -----------------------------------------------------------------------------

func TestLoggerAdapterInfo(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Info("info msg")
	assert.Equal(t, "info", spy.lastMethod)
	assert.Equal(t, "info msg", spy.lastMsg)
}

func TestLoggerAdapterInfof(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Infof("hello %s", "world")
	assert.Equal(t, "info", spy.lastMethod)
	assert.Equal(t, "hello world", spy.lastMsg)
}

func TestLoggerAdapterInfoContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.InfoContext(context.Background(), "ctx info")
	assert.Equal(t, "info", spy.lastMethod)
	assert.Equal(t, "ctx info", spy.lastMsg)
}

func TestLoggerAdapterInfofContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.InfofContext(context.Background(), "ctx %s", "infof")
	assert.Equal(t, "info", spy.lastMethod)
	assert.Equal(t, "ctx infof", spy.lastMsg)
}

// -----------------------------------------------------------------------------
// Warn family
// -----------------------------------------------------------------------------

func TestLoggerAdapterWarn(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Warn("warn msg")
	assert.Equal(t, "warn", spy.lastMethod)
	assert.Equal(t, "warn msg", spy.lastMsg)
}

func TestLoggerAdapterWarnf(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Warnf("hello %s", "world")
	assert.Equal(t, "warn", spy.lastMethod)
	assert.Equal(t, "hello world", spy.lastMsg)
}

func TestLoggerAdapterWarnContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.WarnContext(context.Background(), "ctx warn")
	assert.Equal(t, "warn", spy.lastMethod)
	assert.Equal(t, "ctx warn", spy.lastMsg)
}

func TestLoggerAdapterWarnfContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.WarnfContext(context.Background(), "ctx %s", "warnf")
	assert.Equal(t, "warn", spy.lastMethod)
	assert.Equal(t, "ctx warnf", spy.lastMsg)
}

// -----------------------------------------------------------------------------
// Error family
// -----------------------------------------------------------------------------

func TestLoggerAdapterError(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Error("error msg")
	assert.Equal(t, "error", spy.lastMethod)
	assert.Equal(t, "error msg", spy.lastMsg)
}

func TestLoggerAdapterErrorf(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.Errorf("hello %s", "world")
	assert.Equal(t, "error", spy.lastMethod)
	assert.Equal(t, "hello world", spy.lastMsg)
}

func TestLoggerAdapterErrorContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.ErrorContext(context.Background(), "ctx error")
	assert.Equal(t, "error", spy.lastMethod)
	assert.Equal(t, "ctx error", spy.lastMsg)
}

func TestLoggerAdapterErrorfContext(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	a.ErrorfContext(context.Background(), "ctx %s", "errorf")
	assert.Equal(t, "error", spy.lastMethod)
	assert.Equal(t, "ctx errorf", spy.lastMsg)
}

// -----------------------------------------------------------------------------
// Panic family
// -----------------------------------------------------------------------------

func TestLoggerAdapterPanic(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	assert.Panics(t, func() {
		a.Panic("panic msg")
	})
	assert.Equal(t, "error", spy.lastMethod)
	assert.Equal(t, "panic msg", spy.lastMsg)
}

func TestLoggerAdapterPanicf(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	assert.Panics(t, func() {
		a.Panicf("panic %s", "formatted")
	})
	assert.Equal(t, "error", spy.lastMethod)
	assert.Equal(t, "panic formatted", spy.lastMsg)
}

// -----------------------------------------------------------------------------
// Fatal family — subprocess tests (os.Exit(1) cannot be tested in-process)
// -----------------------------------------------------------------------------

func TestLoggerAdapterFatal(t *testing.T) {
	if os.Getenv("GO_TEST_ADAPTER_FATAL") == "1" {
		a := newAdapter(&spyLogger{})
		a.Fatal("fatal msg")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestLoggerAdapterFatalHelperProcess$", "-test.v") // #nosec G204 G702
	cmd.Env = append(os.Environ(), "GO_TEST_ADAPTER_FATAL=1")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.NotEmpty(t, out)
}

func TestLoggerAdapterFatalHelperProcess(t *testing.T) {
	if os.Getenv("GO_TEST_ADAPTER_FATAL") != "1" {
		t.Skip("helper process")
	}
	a := newAdapter(&spyLogger{})
	a.Fatal("fatal msg")
}

func TestLoggerAdapterFatalf(t *testing.T) {
	if os.Getenv("GO_TEST_ADAPTER_FATALF") == "1" {
		a := newAdapter(&spyLogger{})
		a.Fatalf("fatal %s", "formatted")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestLoggerAdapterFatalfHelperProcess$", "-test.v") // #nosec G204 G702
	cmd.Env = append(os.Environ(), "GO_TEST_ADAPTER_FATALF=1")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.NotEmpty(t, out)
}

func TestLoggerAdapterFatalfHelperProcess(t *testing.T) {
	if os.Getenv("GO_TEST_ADAPTER_FATALF") != "1" {
		t.Skip("helper process")
	}
	a := newAdapter(&spyLogger{})
	a.Fatalf("fatal %s", "formatted")
}

// -----------------------------------------------------------------------------
// Utility methods
// -----------------------------------------------------------------------------

func TestLoggerAdapterLogLevel(t *testing.T) {
	t.Run("defaults to InfoLevel without LeveledLogger", func(t *testing.T) {
		a := newAdapter(&spyLogger{})
		assert.Equal(t, log.InfoLevel, a.LogLevel())
	})

	t.Run("uses LeveledLogger level when implemented", func(t *testing.T) {
		a := newLoggerAdapter(&leveledSpyLogger{level: "debug"})
		assert.Equal(t, log.DebugLevel, a.LogLevel())
	})

	t.Run("unrecognized LeveledLogger value defaults to InfoLevel", func(t *testing.T) {
		a := newLoggerAdapter(&leveledSpyLogger{level: "unknown"})
		assert.Equal(t, log.InfoLevel, a.LogLevel())
	})
}

func TestLoggerAdapterEnabled(t *testing.T) {
	t.Run("default InfoLevel gates debug but allows info and above", func(t *testing.T) {
		a := newAdapter(&spyLogger{})
		assert.False(t, a.Enabled(log.DebugLevel))
		assert.True(t, a.Enabled(log.InfoLevel))
		assert.True(t, a.Enabled(log.WarningLevel))
		assert.True(t, a.Enabled(log.ErrorLevel))
		assert.True(t, a.Enabled(log.FatalLevel))
		assert.True(t, a.Enabled(log.PanicLevel))
	})

	t.Run("DebugLevel enables all levels", func(t *testing.T) {
		a := newLoggerAdapter(&leveledSpyLogger{level: "debug"})
		assert.True(t, a.Enabled(log.DebugLevel))
		assert.True(t, a.Enabled(log.InfoLevel))
		assert.True(t, a.Enabled(log.ErrorLevel))
	})

	t.Run("ErrorLevel gates debug, info, and warning", func(t *testing.T) {
		a := newLoggerAdapter(&leveledSpyLogger{level: "error"})
		assert.False(t, a.Enabled(log.DebugLevel))
		assert.False(t, a.Enabled(log.InfoLevel))
		assert.False(t, a.Enabled(log.WarningLevel))
		assert.True(t, a.Enabled(log.ErrorLevel))
		assert.True(t, a.Enabled(log.FatalLevel))
	})
}

func TestLoggerAdapterWith(t *testing.T) {
	t.Run("no args returns same adapter", func(t *testing.T) {
		a := newAdapter(&spyLogger{})
		result := a.With()
		assert.Same(t, a, result)
	})
	t.Run("returns new adapter with accumulated fields", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		child := a.With("tenant", "t1")
		assert.NotSame(t, a, child)

		// child should carry the fields
		child.Info("hello")
		assert.Equal(t, "info", spy.lastMethod)
		assert.Equal(t, "hello", spy.lastMsg)
		assert.Equal(t, []any{"tenant", "t1"}, spy.lastFields)

		// original adapter should not have the fields
		a.Info("plain")
		assert.Equal(t, "plain", spy.lastMsg)
		assert.Empty(t, spy.lastFields)
	})
	t.Run("fields accumulate across chained With calls", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		child := a.With("k1", "v1").With("k2", "v2")
		child.Info("msg")
		assert.Equal(t, []any{"k1", "v1", "k2", "v2"}, spy.lastFields)
	})
	t.Run("preserves LogLevel and Enabled", func(t *testing.T) {
		spy := &leveledSpyLogger{spyLogger: spyLogger{}, level: "error"}
		a := newLoggerAdapter(spy)
		child := a.With("key", "value")
		la := child.(*loggerAdapter)
		assert.Equal(t, log.ErrorLevel, la.LogLevel())
		assert.True(t, la.Enabled(log.ErrorLevel))
		assert.False(t, la.Enabled(log.DebugLevel))
	})
	t.Run("fields are prepended to formatted log methods", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		child := a.With("rid", "123")
		child.(*loggerAdapter).Debugf("count=%d", 5)
		assert.Equal(t, "debug", spy.lastMethod)
		assert.Equal(t, "count=5", spy.lastMsg)
		assert.Equal(t, []any{"rid", "123"}, spy.lastFields)
	})
}

func TestLoggerAdapterLogOutput(t *testing.T) {
	a := newAdapter(&spyLogger{})
	assert.Nil(t, a.LogOutput())
}

func TestLoggerAdapterFlush(t *testing.T) {
	a := newAdapter(&spyLogger{})
	assert.NoError(t, a.Flush())
}

func TestLoggerAdapterStdLogger(t *testing.T) {
	spy := &spyLogger{}
	a := newAdapter(spy)
	std := a.StdLogger()
	require.NotNil(t, std)
	std.Print("std message")
	assert.Equal(t, "info", spy.lastMethod)
	assert.Contains(t, spy.lastMsg, "std message")
	assert.False(t, strings.HasSuffix(spy.lastMsg, "\n"), "trailing newline should be trimmed")
}

// -----------------------------------------------------------------------------
// goaktArgsToMsg helper
// -----------------------------------------------------------------------------

func TestGoaktArgsToMsg(t *testing.T) {
	t.Run("empty args returns empty message and nil fields", func(t *testing.T) {
		msg, fields := goaktArgsToMsg(nil)
		assert.Equal(t, "", msg)
		assert.Nil(t, fields)
	})

	t.Run("single arg returns it as message with nil fields", func(t *testing.T) {
		msg, fields := goaktArgsToMsg([]any{"hello"})
		assert.Equal(t, "hello", msg)
		assert.Nil(t, fields)
	})

	t.Run("multiple args splits message from key-value fields", func(t *testing.T) {
		msg, fields := goaktArgsToMsg([]any{"hello", "key", "value"})
		assert.Equal(t, "hello", msg)
		assert.Equal(t, []any{"key", "value"}, fields)
	})
}

// -----------------------------------------------------------------------------
// Multi-arg routing — verifies fields are passed through, not swallowed
// -----------------------------------------------------------------------------

func TestLoggerAdapterMultiArgRouting(t *testing.T) {
	t.Run("Debug passes fields to inner logger", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		a.Debug("msg", "k", "v")
		assert.Equal(t, "msg", spy.lastMsg)
		assert.Equal(t, []any{"k", "v"}, spy.lastFields)
	})

	t.Run("Info passes fields to inner logger", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		a.Info("msg", "k", "v")
		assert.Equal(t, "msg", spy.lastMsg)
		assert.Equal(t, []any{"k", "v"}, spy.lastFields)
	})

	t.Run("Warn passes fields to inner logger", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		a.Warn("msg", "k", "v")
		assert.Equal(t, "msg", spy.lastMsg)
		assert.Equal(t, []any{"k", "v"}, spy.lastFields)
	})

	t.Run("Error passes fields to inner logger", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		a.Error("msg", "k", "v")
		assert.Equal(t, "msg", spy.lastMsg)
		assert.Equal(t, []any{"k", "v"}, spy.lastFields)
	})

	t.Run("Panic passes fields to inner logger before panicking", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		assert.Panics(t, func() { a.Panic("msg", "k", "v") })
		assert.Equal(t, "msg", spy.lastMsg)
		assert.Equal(t, []any{"k", "v"}, spy.lastFields)
	})

	t.Run("empty args produces empty message", func(t *testing.T) {
		spy := &spyLogger{}
		a := newAdapter(spy)
		a.Info()
		assert.Equal(t, "", spy.lastMsg)
		assert.Nil(t, spy.lastFields)
	})
}

// -----------------------------------------------------------------------------
// isNilLogger helper
// -----------------------------------------------------------------------------

func TestIsNilLogger(t *testing.T) {
	t.Run("untyped nil returns true", func(t *testing.T) {
		assert.True(t, isNilLogger(nil))
	})

	t.Run("typed-nil pointer returns true", func(t *testing.T) {
		var spy *spyLogger // typed-nil
		assert.True(t, isNilLogger(spy))
	})

	t.Run("non-nil pointer returns false", func(t *testing.T) {
		assert.False(t, isNilLogger(&spyLogger{}))
	})

	t.Run("value type returns false", func(t *testing.T) {
		assert.False(t, isNilLogger(noopLogger{}))
	})
}

// -----------------------------------------------------------------------------
// parseLevel helper
// -----------------------------------------------------------------------------

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  log.Level
	}{
		{"debug", log.DebugLevel},
		{"DEBUG", log.DebugLevel},
		{"info", log.InfoLevel},
		{"warn", log.WarningLevel},
		{"warning", log.WarningLevel},
		{"error", log.ErrorLevel},
		{"fatal", log.FatalLevel},
		{"panic", log.PanicLevel},
		{"unknown", log.InfoLevel},
		{"", log.InfoLevel},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, parseLevel(tt.input))
		})
	}
}

// -----------------------------------------------------------------------------
// levelSeverity helper
// -----------------------------------------------------------------------------

func TestLevelSeverity(t *testing.T) {
	assert.Less(t, levelSeverity(log.DebugLevel), levelSeverity(log.InfoLevel))
	assert.Less(t, levelSeverity(log.InfoLevel), levelSeverity(log.WarningLevel))
	assert.Less(t, levelSeverity(log.WarningLevel), levelSeverity(log.ErrorLevel))
	assert.Less(t, levelSeverity(log.ErrorLevel), levelSeverity(log.FatalLevel))
	assert.Less(t, levelSeverity(log.FatalLevel), levelSeverity(log.PanicLevel))
}

// -----------------------------------------------------------------------------
// loggerWriter newline trimming
// -----------------------------------------------------------------------------

func TestLoggerWriterTrimsNewline(t *testing.T) {
	spy := &spyLogger{}
	w := &loggerWriter{inner: spy}
	n, err := w.Write([]byte("hello\n"))
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, "hello", spy.lastMsg)
	assert.False(t, strings.HasSuffix(spy.lastMsg, "\n"))
}

func TestLoggerWriterTrimsCRLF(t *testing.T) {
	spy := &spyLogger{}
	w := &loggerWriter{inner: spy}
	n, err := w.Write([]byte("hello\r\n"))
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.Equal(t, "hello", spy.lastMsg)
}

func TestLoggerWriterNoNewline(t *testing.T) {
	spy := &spyLogger{}
	w := &loggerWriter{inner: spy}
	n, err := w.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", spy.lastMsg)
}

// noopLogger is a Logger implementation that discards all output. It is used
// in tests to exercise WithLogger with a concrete Logger value; goaktlog is
// still imported in this file for unrelated assertions.
type noopLogger struct{}

func (noopLogger) Debug(_ string, _ ...any) {}
func (noopLogger) Info(_ string, _ ...any)  {}
func (noopLogger) Warn(_ string, _ ...any)  {}
func (noopLogger) Error(_ string, _ ...any) {}

// noopPtrLogger is a pointer-receiver Logger used to test typed-nil detection.
type noopPtrLogger struct{}

func (*noopPtrLogger) Debug(_ string, _ ...any) {}
func (*noopPtrLogger) Info(_ string, _ ...any)  {}
func (*noopPtrLogger) Warn(_ string, _ ...any)  {}
func (*noopPtrLogger) Error(_ string, _ ...any) {}

// leveledNoopLogger implements both Logger and LeveledLogger.
type leveledNoopLogger struct {
	level string
}

func (*leveledNoopLogger) Debug(_ string, _ ...any) {}
func (*leveledNoopLogger) Info(_ string, _ ...any)  {}
func (*leveledNoopLogger) Warn(_ string, _ ...any)  {}
func (*leveledNoopLogger) Error(_ string, _ ...any) {}
func (l *leveledNoopLogger) Level() string          { return l.level }
