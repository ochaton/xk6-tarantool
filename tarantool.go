package tarantool

import (
	"github.com/tarantool/go-tarantool"
	ttcp "github.com/tarantool/go-tarantool/connection_pool"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/tarantool", new(Tarantool))
}

// Tarantool is the k6 Tarantool extension
type Tarantool struct{}

// Connect creates a new Tarantool connection
func (t Tarantool) Connect(addr string, opts tarantool.Opts) (*ttcp.ConnectionPool, error) {
	if addr == "" {
		addr = "localhost:3301"
	}
	return t.ConnectPool([]string{addr}, opts)
}

// ConnectPool connects to shard of Tarantools
func (Tarantool) ConnectPool(addrs []string, opts tarantool.Opts) (*ttcp.ConnectionPool, error) {
	if len(addrs) == 0 {
		addrs = []string{"localhost:3301"}
	}
	pool, err := ttcp.Connect(addrs, opts)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

// Select performs select to box.space (any node)
func (Tarantool) Select(conn *ttcp.ConnectionPool, space, index interface{}, offset, limit, iterator uint32, key interface{}) (*tarantool.Response, error) {
	resp, err := conn.Select(space, index, offset, limit, iterator, key)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Select performs select to box.space (any node but prefer RW)
func (Tarantool) SelectARW(conn *ttcp.ConnectionPool, space, index interface{}, offset, limit, iterator uint32, key interface{}) (*tarantool.Response, error) {
	resp, err := conn.Select(space, index, offset, limit, iterator, key, ttcp.PreferRW)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Select performs select to box.space (any node but prefer RO)
func (Tarantool) SelectARO(conn *ttcp.ConnectionPool, space, index interface{}, offset, limit, iterator uint32, key interface{}) (*tarantool.Response, error) {
	resp, err := conn.Select(space, index, offset, limit, iterator, key, ttcp.PreferRO)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Insert performs insertion to box.space
func (Tarantool) Insert(conn *ttcp.ConnectionPool, space, data interface{}) (*tarantool.Response, error) {
	resp, err := conn.Insert(space, data)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Replace performs "insert or replace" action to box.space
func (Tarantool) Replace(conn *ttcp.ConnectionPool, space, data interface{}) (*tarantool.Response, error) {
	resp, err := conn.Replace(space, data)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Delete performs deletion of a tuple by key
func (Tarantool) Delete(conn *ttcp.ConnectionPool, space, index, key interface{}) (*tarantool.Response, error) {
	resp, err := conn.Delete(space, index, key)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Update performs update of a tuple by key
func (Tarantool) Update(conn *ttcp.ConnectionPool, space, index, key, ops interface{}) (*tarantool.Response, error) {
	resp, err := conn.Update(space, index, key, ops)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Upsert performs "update or insert" action of a tuple by key
func (Tarantool) Upsert(conn *ttcp.ConnectionPool, space, tuple, ops interface{}) (*tarantool.Response, error) {
	resp, err := conn.Upsert(space, tuple, ops)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Call calls registered tarantool function (calls on any but prefer RW)
func (Tarantool) Call(conn *ttcp.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Call(fnName, args, ttcp.PreferRW)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Call calls registered tarantool function (calls on RW)
func (Tarantool) CallARW(conn *ttcp.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Call(fnName, args, ttcp.PreferRW)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Call calls registered tarantool function (calls on any but prefer RO)
func (Tarantool) CallARO(conn *ttcp.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Call(fnName, args, ttcp.PreferRO)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Call17 calls registered tarantool function (Call17 on any but prefer RW)
func (Tarantool) Call17(conn *ttcp.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Call17(fnName, args, ttcp.PreferRW)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Call17 calls registered tarantool function (Call17 on any but prefer RW)
func (Tarantool) Call17ARO(conn *ttcp.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Call17(fnName, args, ttcp.PreferRO)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Call17 calls registered tarantool function (Call17 on any but prefer RW)
func (Tarantool) Call17RW(conn *ttcp.ConnectionPool, fnName string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Call17(fnName, args, ttcp.RW)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Eval passes lua expression for evaluation (calls on any but prefers RW)
func (Tarantool) Eval(conn *ttcp.ConnectionPool, expr string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Eval(expr, args, ttcp.PreferRW)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Eval passes lua expression for evaluation
func (Tarantool) EvalARW(conn *ttcp.ConnectionPool, expr string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Eval(expr, args, ttcp.PreferRW)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Eval passes lua expression for evaluation (calls on any but prefers RO)
func (Tarantool) EvalARO(conn *ttcp.ConnectionPool, expr string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Eval(expr, args, ttcp.PreferRO)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// Eval passes lua expression for evaluation (calls on RW)
func (Tarantool) EvalRW(conn *ttcp.ConnectionPool, expr string, args interface{}) (*tarantool.Response, error) {
	resp, err := conn.Eval(expr, args, ttcp.RW)
	if err != nil {
		return nil, err
	}
	return resp, err
}
