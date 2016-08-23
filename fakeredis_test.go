package main

import (
	"testing"
)

type MockValue struct {
	Act    string
	Op     string
	Key    interface{}
	Values []interface{}
}
type MockRedis struct {
	t      *testing.T
	Oprs   []*MockValue
	Closed bool
}

func NewMockRedis(t *testing.T) *MockRedis {
	return &MockRedis{t, nil, false}
}

func (m *MockRedis) Send(Command string, vals ...interface{}) error {
	if m.Closed {
		m.t.Error("redis send called while closed")
	}

	m.Oprs = append(m.Oprs, m.createValue("SEND", Command, vals...))
	return nil
}

func (m *MockRedis) Do(Command string, vals ...interface{}) (interface{}, error) {
	if m.Closed {
		m.t.Error("redis Do called while closed")
	}
	m.Oprs = append(m.Oprs, m.createValue("DO", Command, vals...))
	return m.Receive()
}

func (m *MockRedis) createValue(Opr, Command string, vals ...interface{}) *MockValue {
	val := &MockValue{Opr, Command, nil, nil}
	if len(vals) > 0 {
		val.Key = vals[0].(string)
		if len(vals) > 1 {
			val.Values = vals[1:]
		}
	}
	return val
}

func (m *MockRedis) Close() error {
	m.Closed = true
	return nil
}

func (m *MockRedis) Flush() error {
	return nil
}

func (m *MockRedis) Err() error {
	return nil
}

func (m *MockRedis) Receive() (reply interface{}, err error) {
	return nil, nil
}

func (m *MockRedis) CheckActAndCommand(idx int, act, cmd string, vals ...interface{}) {

	if len(m.Oprs) <= idx {
		m.t.Error("expected", act, cmd, "at", idx, "found none")
		return
	}

	opr := m.Oprs[idx]

	if opr.Act != act {
		m.t.Error("expected", act, "at", idx, "found", opr.Act)
	}

	if opr.Op != cmd {
		m.t.Error("expected", cmd, "at", idx, "found", opr.Op)
	}

	oprLen := 0
	if opr.Key != nil {
		oprLen = 1
	}

	if len(vals) != len(opr.Values)+oprLen {
		m.t.Error("expected", len(vals), "values at", idx, "found", len(opr.Values))
	}

	rvals := vals[oprLen:]

	for i, val := range rvals {
		if len(opr.Values) <= i {
			break
		}

		if val.(string) != opr.Values[i].(string) {
			m.t.Errorf("expected value[%d] = %s found %s\n", i, val.(string), opr.Values[i].(string))
		}
	}
}
