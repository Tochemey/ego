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

// SupervisorDirective defines the supervisor directive
//
// It represents the action that a supervisor can take when an entity fails or panics
// during message processing. Each directive corresponds to a specific recovery behavior:
//
//   - StopDirective: Instructs the supervisor to stop the failing actor.
//     allowing it to continue processing messages (typically used for recoverable errors).
//   - RestartDirective: Instructs the supervisor to restart the failing actor, reinitializing its state.
type SupervisorDirective int

const (
	// StopDirective indicates that when an entity fails, the supervisor should immediately stop
	// the entity. This directive is typically used when a failure is deemed irrecoverable
	// or when the entity's state cannot be safely resumed.
	StopDirective SupervisorDirective = iota

	// RestartDirective indicates that when an entity fails, the supervisor should restart the entity.
	// Restarting involves stopping the current instance and creating a new one, effectively resetting
	// the entity's internal state.
	RestartDirective
)
