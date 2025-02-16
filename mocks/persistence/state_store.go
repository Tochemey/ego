// Code generated by mockery. DO NOT EDIT.

package persistence

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
	egopb "github.com/tochemey/ego/v3/egopb"
)

// StateStore is an autogenerated mock type for the StateStore type
type StateStore struct {
	mock.Mock
}

type StateStore_Expecter struct {
	mock *mock.Mock
}

func (_m *StateStore) EXPECT() *StateStore_Expecter {
	return &StateStore_Expecter{mock: &_m.Mock}
}

// Connect provides a mock function with given fields: ctx
func (_m *StateStore) Connect(ctx context.Context) error {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Connect")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StateStore_Connect_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Connect'
type StateStore_Connect_Call struct {
	*mock.Call
}

// Connect is a helper method to define mock.On call
//   - ctx context.Context
func (_e *StateStore_Expecter) Connect(ctx interface{}) *StateStore_Connect_Call {
	return &StateStore_Connect_Call{Call: _e.mock.On("Connect", ctx)}
}

func (_c *StateStore_Connect_Call) Run(run func(ctx context.Context)) *StateStore_Connect_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *StateStore_Connect_Call) Return(_a0 error) *StateStore_Connect_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StateStore_Connect_Call) RunAndReturn(run func(context.Context) error) *StateStore_Connect_Call {
	_c.Call.Return(run)
	return _c
}

// Disconnect provides a mock function with given fields: ctx
func (_m *StateStore) Disconnect(ctx context.Context) error {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Disconnect")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StateStore_Disconnect_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Disconnect'
type StateStore_Disconnect_Call struct {
	*mock.Call
}

// Disconnect is a helper method to define mock.On call
//   - ctx context.Context
func (_e *StateStore_Expecter) Disconnect(ctx interface{}) *StateStore_Disconnect_Call {
	return &StateStore_Disconnect_Call{Call: _e.mock.On("Disconnect", ctx)}
}

func (_c *StateStore_Disconnect_Call) Run(run func(ctx context.Context)) *StateStore_Disconnect_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *StateStore_Disconnect_Call) Return(_a0 error) *StateStore_Disconnect_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StateStore_Disconnect_Call) RunAndReturn(run func(context.Context) error) *StateStore_Disconnect_Call {
	_c.Call.Return(run)
	return _c
}

// GetLatestState provides a mock function with given fields: ctx, persistenceID
func (_m *StateStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	ret := _m.Called(ctx, persistenceID)

	if len(ret) == 0 {
		panic("no return value specified for GetLatestState")
	}

	var r0 *egopb.DurableState
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (*egopb.DurableState, error)); ok {
		return rf(ctx, persistenceID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) *egopb.DurableState); ok {
		r0 = rf(ctx, persistenceID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*egopb.DurableState)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, persistenceID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// StateStore_GetLatestState_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetLatestState'
type StateStore_GetLatestState_Call struct {
	*mock.Call
}

// GetLatestState is a helper method to define mock.On call
//   - ctx context.Context
//   - persistenceID string
func (_e *StateStore_Expecter) GetLatestState(ctx interface{}, persistenceID interface{}) *StateStore_GetLatestState_Call {
	return &StateStore_GetLatestState_Call{Call: _e.mock.On("GetLatestState", ctx, persistenceID)}
}

func (_c *StateStore_GetLatestState_Call) Run(run func(ctx context.Context, persistenceID string)) *StateStore_GetLatestState_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *StateStore_GetLatestState_Call) Return(_a0 *egopb.DurableState, _a1 error) *StateStore_GetLatestState_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *StateStore_GetLatestState_Call) RunAndReturn(run func(context.Context, string) (*egopb.DurableState, error)) *StateStore_GetLatestState_Call {
	_c.Call.Return(run)
	return _c
}

// Ping provides a mock function with given fields: ctx
func (_m *StateStore) Ping(ctx context.Context) error {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for Ping")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StateStore_Ping_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Ping'
type StateStore_Ping_Call struct {
	*mock.Call
}

// Ping is a helper method to define mock.On call
//   - ctx context.Context
func (_e *StateStore_Expecter) Ping(ctx interface{}) *StateStore_Ping_Call {
	return &StateStore_Ping_Call{Call: _e.mock.On("Ping", ctx)}
}

func (_c *StateStore_Ping_Call) Run(run func(ctx context.Context)) *StateStore_Ping_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *StateStore_Ping_Call) Return(_a0 error) *StateStore_Ping_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StateStore_Ping_Call) RunAndReturn(run func(context.Context) error) *StateStore_Ping_Call {
	_c.Call.Return(run)
	return _c
}

// WriteState provides a mock function with given fields: ctx, state
func (_m *StateStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	ret := _m.Called(ctx, state)

	if len(ret) == 0 {
		panic("no return value specified for WriteState")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *egopb.DurableState) error); ok {
		r0 = rf(ctx, state)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// StateStore_WriteState_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'WriteState'
type StateStore_WriteState_Call struct {
	*mock.Call
}

// WriteState is a helper method to define mock.On call
//   - ctx context.Context
//   - state *egopb.DurableState
func (_e *StateStore_Expecter) WriteState(ctx interface{}, state interface{}) *StateStore_WriteState_Call {
	return &StateStore_WriteState_Call{Call: _e.mock.On("WriteState", ctx, state)}
}

func (_c *StateStore_WriteState_Call) Run(run func(ctx context.Context, state *egopb.DurableState)) *StateStore_WriteState_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(*egopb.DurableState))
	})
	return _c
}

func (_c *StateStore_WriteState_Call) Return(_a0 error) *StateStore_WriteState_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *StateStore_WriteState_Call) RunAndReturn(run func(context.Context, *egopb.DurableState) error) *StateStore_WriteState_Call {
	_c.Call.Return(run)
	return _c
}

// NewStateStore creates a new instance of StateStore. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewStateStore(t interface {
	mock.TestingT
	Cleanup(func())
}) *StateStore {
	mock := &StateStore{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
