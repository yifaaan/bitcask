package main

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
	"github.com/yifaaan/bitcask"
	bitcask_rds "github.com/yifaaan/bitcask/redis"
)

func openTestClient(t *testing.T) *BitcaskClient {
	t.Helper()

	options := bitcask.DefaultOptions
	options.DirPath = t.TempDir()
	options.SyncWrite = true

	db, err := bitcask_rds.NewRedisDatastruct(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return &BitcaskClient{db: db}
}

func commandArgs(values ...string) [][]byte {
	args := make([][]byte, len(values))
	for i, value := range values {
		args[i] = []byte(value)
	}
	return args
}

func runSupportedCommand(t *testing.T, client *BitcaskClient, command string, args ...string) (any, error) {
	t.Helper()

	handler, ok := supportedCommands[command]
	require.Truef(t, ok, "command %q is not registered", command)
	return handler(client, commandArgs(args...))
}

func TestSupportedCommands(t *testing.T) {
	want := []string{
		"set", "get", "del", "type",
		"hset", "hget", "hdel",
		"sadd", "sismember", "srem",
		"lpush", "rpush", "lpop", "rpop",
		"zadd", "zscore",
	}

	for _, command := range want {
		assert.Contains(t, supportedCommands, command)
	}
}

func TestClientStringAndKeyCommands(t *testing.T) {
	client := openTestClient(t)

	result, err := runSupportedCommand(t, client, "set", "name", "bitcask")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleString("OK"), result)

	result, err = runSupportedCommand(t, client, "get", "name")
	require.NoError(t, err)
	assert.Equal(t, []byte("bitcask"), result)

	result, err = runSupportedCommand(t, client, "type", "name")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleString("string"), result)

	result, err = runSupportedCommand(t, client, "del", "name")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)

	result, err = runSupportedCommand(t, client, "del", "name")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(0), result)

	_, err = runSupportedCommand(t, client, "get")
	assert.EqualError(t, err, "wrong number of arguments for 'get' command")
}

func TestClientCollectionCommands(t *testing.T) {
	client := openTestClient(t)

	result, err := runSupportedCommand(t, client, "hset", "user:1", "name", "Alice")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)
	result, err = runSupportedCommand(t, client, "hget", "user:1", "name")
	require.NoError(t, err)
	assert.Equal(t, []byte("Alice"), result)
	result, err = runSupportedCommand(t, client, "hdel", "user:1", "name")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)

	result, err = runSupportedCommand(t, client, "sadd", "tags", "go")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)
	result, err = runSupportedCommand(t, client, "sismember", "tags", "go")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)
	result, err = runSupportedCommand(t, client, "srem", "tags", "go")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)

	result, err = runSupportedCommand(t, client, "lpush", "queue", "first")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)
	result, err = runSupportedCommand(t, client, "rpush", "queue", "last")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(2), result)
	result, err = runSupportedCommand(t, client, "lpop", "queue")
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), result)
	result, err = runSupportedCommand(t, client, "rpop", "queue")
	require.NoError(t, err)
	assert.Equal(t, []byte("last"), result)

	result, err = runSupportedCommand(t, client, "zadd", "scores", "10.5", "Alice")
	require.NoError(t, err)
	assert.Equal(t, redcon.SimpleInt(1), result)
	result, err = runSupportedCommand(t, client, "zscore", "scores", "Alice")
	require.NoError(t, err)
	assert.Equal(t, 10.5, result)

	_, err = runSupportedCommand(t, client, "zadd", "scores", "not-a-number", "Alice")
	assert.EqualError(t, err, "value is not a valid float")
}

func TestExecClientCommand(t *testing.T) {
	client := openTestClient(t)

	conn := &recordingConn{context: client}
	execClientCommand(conn, redcon.Command{Args: commandArgs("PiNg")})
	assert.Equal(t, []string{"PONG"}, conn.strings)

	conn = &recordingConn{context: client}
	execClientCommand(conn, redcon.Command{Args: commandArgs("PING", "hello")})
	assert.Equal(t, [][]byte{[]byte("hello")}, conn.bulks)

	conn = &recordingConn{context: client}
	execClientCommand(conn, redcon.Command{Args: commandArgs("SET", "name", "bitcask")})
	assert.Equal(t, []any{redcon.SimpleString("OK")}, conn.anys)

	conn = &recordingConn{context: client}
	execClientCommand(conn, redcon.Command{Args: commandArgs("GET", "name")})
	assert.Equal(t, []any{[]byte("bitcask")}, conn.anys)

	conn = &recordingConn{context: client}
	execClientCommand(conn, redcon.Command{Args: commandArgs("GET")})
	assert.Equal(t, []string{"wrong number of arguments for 'get' command"}, conn.errors)

	conn = &recordingConn{context: client}
	execClientCommand(conn, redcon.Command{Args: commandArgs("UNKNOWN")})
	assert.Equal(t, []string{"command not supported: unknown"}, conn.errors)

	conn = &recordingConn{context: client}
	execClientCommand(conn, redcon.Command{Args: commandArgs("QUIT")})
	assert.Equal(t, []string{"OK"}, conn.strings)
	assert.True(t, conn.closed)
}

type recordingConn struct {
	context interface{}
	strings []string
	bulks   [][]byte
	errors  []string
	anys    []any
	nulls   int
	closed  bool
}

func (c *recordingConn) RemoteAddr() string                   { return "test" }
func (c *recordingConn) Close() error                         { c.closed = true; return nil }
func (c *recordingConn) WriteError(message string)            { c.errors = append(c.errors, message) }
func (c *recordingConn) WriteString(value string)             { c.strings = append(c.strings, value) }
func (c *recordingConn) WriteBulk(value []byte)               { c.bulks = append(c.bulks, value) }
func (c *recordingConn) WriteBulkString(value string)         { c.bulks = append(c.bulks, []byte(value)) }
func (c *recordingConn) WriteInt(int)                         {}
func (c *recordingConn) WriteInt64(int64)                     {}
func (c *recordingConn) WriteUint64(uint64)                   {}
func (c *recordingConn) WriteArray(int)                       {}
func (c *recordingConn) WriteNull()                           { c.nulls++ }
func (c *recordingConn) WriteRaw([]byte)                      {}
func (c *recordingConn) WriteAny(value interface{})           { c.anys = append(c.anys, value) }
func (c *recordingConn) Context() interface{}                 { return c.context }
func (c *recordingConn) SetContext(value interface{})         { c.context = value }
func (c *recordingConn) SetReadBuffer(int)                    {}
func (c *recordingConn) Detach() redcon.DetachedConn          { return nil }
func (c *recordingConn) ReadPipeline() []redcon.Command       { return nil }
func (c *recordingConn) PeekPipeline() []redcon.Command       { return nil }
func (c *recordingConn) NetConn() net.Conn                    { return nil }
func (c *recordingConn) WriteBulkFrom(int64, io.Reader)       {}
func (c *recordingConn) ReadCommand() (redcon.Command, error) { return redcon.Command{}, nil }
func (c *recordingConn) Flush() error                         { return nil }
