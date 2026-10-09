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

import "errors"

// Rejection is an error a command handler returns to refuse a command for a
// business reason, such as an overdrawn account or an invalid amount.
//
// The code of a Rejection travels with the command reply. The error that
// [Engine.SendCommand] returns for a rejected command is therefore a
// *Rejection carrying the same code, wherever the entity runs in the
// cluster. Callers tell rejections apart by code, either with [errors.Is]
// against a Rejection declared once by the application, or with [errors.As]
// and [Rejection.Code]:
//
//	var ErrInsufficientFunds = ego.NewRejection("insufficient_funds", "insufficient funds")
//
//	// in the command handler
//	return nil, fmt.Errorf("account %s: %w", accountID, ErrInsufficientFunds)
//
//	// in the caller
//	_, _, err := engine.SendCommand(ctx, accountID, command, timeout)
//	if errors.Is(err, ErrInsufficientFunds) {
//		// refuse the withdrawal
//	}
//
// A handler may return a Rejection as is or wrapped. The caller receives the
// text of the returned error as the message, and the code of the outermost
// Rejection found in its chain.
//
// A Rejection is also the error a [SagaBehavior] receives in HandleError when
// a participant rejects a command, so sagas can branch on it the same way.
type Rejection struct {
	code    string
	message string
}

// enforce compilation error
var _ error = (*Rejection)(nil)

// NewRejection creates a Rejection with the given code and message.
//
// The code identifies the kind of rejection and is what callers match on, so
// it must be stable and non-empty. A Rejection with an empty code reaches the
// caller as a plain error. The message is the human-readable explanation.
func NewRejection(code, message string) *Rejection {
	return &Rejection{
		code:    code,
		message: message,
	}
}

// Code returns the code identifying the kind of rejection.
func (rejection *Rejection) Code() string {
	return rejection.code
}

// Error returns the message of the rejection.
func (rejection *Rejection) Error() string {
	return rejection.message
}

// Is reports whether target is a *Rejection with the same non-empty code,
// which lets [errors.Is] match a rejection received from an entity against
// the one the application declared. A Rejection with an empty code matches
// nothing, since it identifies no kind of rejection.
func (rejection *Rejection) Is(target error) bool {
	other, ok := target.(*Rejection)
	return ok && other != nil && rejection != nil && rejection.code != "" && other.code == rejection.code
}

// rejectionCode returns the code of the outermost Rejection in the chain of
// err, or an empty string when err carries no Rejection.
func rejectionCode(err error) string {
	var rejection *Rejection
	if errors.As(err, &rejection) {
		return rejection.code
	}

	return ""
}
