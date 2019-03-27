package conn

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	PoolClosed    = errors.New("conn pool is closed")
	InvalidConfig = errors.New("invalid config")
)

type builder func() (*Poolable, error)

type Poolable struct {
	Conn io.Closer
	context.Context
}

type Conn struct {
	notice  chan struct{}  // 关闭信号
	pool    chan *Poolable // 可关闭连接池
	max     int            // 池容量
	active  int            // 可用的连接数
	closed  bool           // 池是否已关闭
	builder builder        // 构造连接
	mutex   *sync.Mutex
}

// 获取连接
func (conn *Conn) Acquire() (*Poolable, error) {
	if conn.closed {
		return nil, PoolClosed
	}
	for {
		closer, err := conn.acquire()
		if err != nil {
			return nil, err
		}
		select {
		case <-closer.Done():
			conn.Close(closer)
			continue
		default:
			return closer, nil
		}
	}
}

func (conn *Conn) acquire() (*Poolable, error) {
acquire:
	select {
	case closer := <-conn.pool:
		return closer, nil
	default:
		conn.mutex.Lock()
		if conn.active >= conn.max {
			conn.mutex.Unlock()
			select {
			case closer := <-conn.pool:
				return closer, nil
			case <-conn.notice:
				goto acquire
			}
		}
		closer, err := conn.builder()
		if err != nil {
			conn.mutex.Unlock()
			return nil, err
		}
		conn.active++
		conn.pool <- closer
		conn.mutex.Unlock()
		return <-conn.pool, nil
	}
}

// 回收连接
func (conn *Conn) Regain(closer *Poolable) error {
	if conn.closed {
		return PoolClosed
	}
	conn.pool <- closer
	return nil
}

// 关闭连接
func (conn *Conn) Close(closer *Poolable) error {
	conn.mutex.Lock()
	err := closer.Conn.Close()
	if err != nil {
		return err
	}
	conn.active--
	if len(conn.notice) == 0 {
		conn.notice <- struct{}{}
	}
	conn.mutex.Unlock()
	return nil
}

// 关闭连接池
func (conn *Conn) Release() error {
	if conn.closed {
		return PoolClosed
	}
	conn.mutex.Lock()
	close(conn.pool)
	for closer := range conn.pool {
		conn.active--
		closer.Conn.Close()
	}
	conn.closed = true
	conn.mutex.Unlock()
	return nil
}

// 创建连接管理器
func NewManager(max int, builder builder) (*Conn, error) {
	if max <= 0 {
		return nil, InvalidConfig
	}
	conn := &Conn{
		notice:  make(chan struct{}, max),
		max:     max,
		pool:    make(chan *Poolable, max),
		closed:  false,
		builder: builder,
		mutex:   new(sync.Mutex),
	}
	for i := 0; i < max; i++ {
		closer, err := builder()
		if err != nil {
			return nil, err
		}
		conn.active++
		conn.pool <- closer
	}
	return conn, nil
}
