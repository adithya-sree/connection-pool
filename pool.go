package pool

import (
	"errors"
	"sync"
	"time"
)

type (
	// ConnectionPool interface
	ConnectionPool interface {
		GetConnection() (Connection, error)
		ReturnConnection(connection Connection)
	}

	// Connection interface
	Connection interface {
		Close() error
	}

	// Pool options
	Options struct {
		ConnectionWaitTimeout int
		NumConnections        int
	}

	// Implementation of ConnectionPool
	DefaultConnectionPool struct {
		mutex                 sync.Mutex
		connectionPool        []Connection
		ConnectionWaitTimeout int
	}

	connectionResponse struct {
		connection Connection
		error      error
	}
)

// Initializes the connection pool
func InitializeConnectionPool(options Options, initialize func() (Connection, error)) (ConnectionPool, error) {
	var pool []Connection
	// Get connection channel
	c := initializeConnections(options, initialize)
	// Initialize all connections
	for i := 0; i < options.NumConnections; i++ {
		if resp := <- c; resp.error != nil {
			// Return error if unable to initialize
			return nil, resp.error
		} else {
			// Append connection to pool
			pool = append(pool, resp.connection)
		}
	}
	// Return connection pool
	return &DefaultConnectionPool{
		mutex:                 sync.Mutex{},
		connectionPool:        pool,
		ConnectionWaitTimeout: options.ConnectionWaitTimeout,
	}, nil
}

// Gets a single connection from the pool
func (d *DefaultConnectionPool) GetConnection() (Connection, error) {
	// Default Timeout
	timer := time.NewTimer(time.Duration(d.ConnectionWaitTimeout) * time.Millisecond)
	cancel := make(chan int, 1)
	// Waits to either receive a connection or to timeout
	select {
	// if timeout is reached, send cancel signal
	// on cancel channel and return error
	case <- timer.C:
		cancel <- 0
		return nil, errors.New("timed out waiting for connection")
	// If connection is received, return connection from pool
	case conn := <- getConnection(d, cancel):
		return conn, nil
	}
}

// Returns a connection to the pool
func (d *DefaultConnectionPool) ReturnConnection(connection Connection) {
	// Lock Mutex
	d.mutex.Lock()
	// Return connection to pool
	d.connectionPool = append(d.connectionPool, connection)
	// Unlock Mutex
	d.mutex.Unlock()
}

// Closes all connections in the pool
func (d *DefaultConnectionPool) Close() {
	// Lock Mutex
	d.mutex.Lock()
	// Return connection to pool
	for _, conn := range d.connectionPool {
		 _ = conn.Close()
	}
	// Unlock Mutex
	d.mutex.Unlock()
}

func initializeConnections(options Options, initialize func() (Connection, error)) <-chan connectionResponse  {
	// Create connection channel
	c := make(chan connectionResponse, 1)
	// Initialize connections
	for i := 0; i < options.NumConnections; i++ {
		// Initialize individual connection
		go func() {
			conn, err := initialize()
			// Return connection to channel
			c <- connectionResponse{
				connection: conn,
				error:      err,
			}
		}()
	}
	// Return channel
	return c
}

func getConnection(pool *DefaultConnectionPool, cancel chan int) <-chan Connection {
	// Create client channel
	c := make(chan Connection, 1)
	// Wait for connection
	go waitForConnection(pool, cancel, c)
	// Return client channel
	return c
}

func waitForConnection(pool *DefaultConnectionPool, cancel chan int, response chan Connection) {
	exit := false
	// Iterate over channel selects until a connection
	// is found or the timeout is reached
	for {
		select {
		// If cancel channel received signal exit iteration
		case <- cancel:
			exit = true
		// If no receive operation is available on channel,
		// attempt to get connection from pool
		default:
			// Lock Mutex
			pool.mutex.Lock()
			if len(pool.connectionPool) >= 1 {
				// If connection exists, return connection on channel
				response <- pool.connectionPool[0]
				// Remove connection from pool
				pool.connectionPool = remove(pool.connectionPool, 0)
				// Exit over iteration
				exit = true
			}
			// Unlock Mutex
			pool.mutex.Unlock()
		}
		// If timeout is reached or connection is found
		// break iteration
		if exit {
			break
		}
	}
}

func remove(a []Connection, i int) []Connection {
	return append(a[:i], a[i+1:]...)
}