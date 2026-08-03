package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/yifaaan/bitcask"
	bitcask_rds "github.com/yifaaan/bitcask/redis"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("wrong number of arguments for '%s' command", cmd)
}

func requireArgs(cmd string, args [][]byte, want int) error {
	if len(args) != want {
		return newWrongNumberOfArgsError(cmd)
	}
	return nil
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (any, error)

var supportedCommands = map[string]cmdHandler{
	"set":       set,
	"get":       get,
	"del":       del,
	"type":      typeCommand,
	"hset":      hset,
	"hget":      hget,
	"hdel":      hdel,
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"lpush":     lpush,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
	"zadd":      zadd,
	"zscore":    zscore,
}

type BitcaskClient struct {
	db     *bitcask_rds.RedisDataStruct
	server *BitcaskServer
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		conn.WriteError("empty command")
		return
	}

	command := strings.ToLower(string(cmd.Args[0]))
	switch command {
	case "ping":
		if len(cmd.Args) > 2 {
			conn.WriteError(newWrongNumberOfArgsError(command).Error())
			return
		}
		if len(cmd.Args) == 2 {
			conn.WriteBulk(cmd.Args[1])
			return
		}
		conn.WriteString("PONG")
		return
	case "quit":
		if len(cmd.Args) != 1 {
			conn.WriteError(newWrongNumberOfArgsError(command).Error())
			return
		}
		conn.WriteString("OK")
		_ = conn.Close()
		return
	}

	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("command not supported: " + command)
		return
	}

	client, ok := conn.Context().(*BitcaskClient)
	if !ok || client == nil || client.db == nil {
		conn.WriteError("client is not initialized")
		return
	}

	res, err := cmdFunc(client, cmd.Args[1:])
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			conn.WriteNull()
		} else {
			conn.WriteError(err.Error())
		}
		return
	}
	conn.WriteAny(res)
}

func set(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("set", args, 2); err != nil {
		return nil, err
	}

	if err := cli.db.Set(args[0], 0, args[1]); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("get", args, 1); err != nil {
		return nil, err
	}

	return cli.db.Get(args[0])
}

func del(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("del", args, 1); err != nil {
		return nil, err
	}

	_, err := cli.db.Type(args[0])
	exists := err == nil
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return nil, err
	}
	if err := cli.db.Del(args[0]); err != nil {
		return nil, err
	}
	if exists {
		return redcon.SimpleInt(1), nil
	}
	return redcon.SimpleInt(0), nil
}

func typeCommand(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("type", args, 1); err != nil {
		return nil, err
	}

	dataType, err := cli.db.Type(args[0])
	if err != nil {
		return nil, err
	}
	name, err := redisTypeName(dataType)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(name), nil
}

func redisTypeName(dataType byte) (string, error) {
	switch dataType {
	case bitcask_rds.STRING:
		return "string", nil
	case bitcask_rds.HASH:
		return "hash", nil
	case bitcask_rds.SET:
		return "set", nil
	case bitcask_rds.LIST:
		return "list", nil
	case bitcask_rds.ZSET:
		return "zset", nil
	default:
		return "", fmt.Errorf("unknown redis data type: %d", dataType)
	}
}

func boolResult(value bool) redcon.SimpleInt {
	if value {
		return redcon.SimpleInt(1)
	}
	return redcon.SimpleInt(0)
}

func hset(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("hset", args, 3); err != nil {
		return nil, err
	}

	isNew, err := cli.db.HSet(args[0], args[1], args[2])
	if err != nil {
		return nil, err
	}
	return boolResult(isNew), nil
}

func hget(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("hget", args, 2); err != nil {
		return nil, err
	}

	return cli.db.HGet(args[0], args[1])
}

func hdel(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("hdel", args, 2); err != nil {
		return nil, err
	}

	deleted, err := cli.db.HDel(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return boolResult(deleted), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("sadd", args, 2); err != nil {
		return nil, err
	}

	isNew, err := cli.db.SAdd(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return boolResult(isNew), nil
}

func sismember(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("sismember", args, 2); err != nil {
		return nil, err
	}

	isMember, err := cli.db.SIsMember(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return boolResult(isMember), nil
}

func srem(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("srem", args, 2); err != nil {
		return nil, err
	}

	removed, err := cli.db.SRem(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return boolResult(removed), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("lpush", args, 2); err != nil {
		return nil, err
	}

	length, err := cli.db.LPush(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(length), nil
}

func rpush(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("rpush", args, 2); err != nil {
		return nil, err
	}

	length, err := cli.db.RPush(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(length), nil
}

func lpop(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("lpop", args, 1); err != nil {
		return nil, err
	}

	return cli.db.LPop(args[0])
}

func rpop(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("rpop", args, 1); err != nil {
		return nil, err
	}

	return cli.db.RPop(args[0])
}

func zadd(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("zadd", args, 3); err != nil {
		return nil, err
	}

	score, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return nil, fmt.Errorf("value is not a valid float")
	}
	isNew, err := cli.db.ZAdd(args[0], score, args[2])
	if err != nil {
		return nil, err
	}
	return boolResult(isNew), nil
}

func zscore(cli *BitcaskClient, args [][]byte) (any, error) {
	if err := requireArgs("zscore", args, 2); err != nil {
		return nil, err
	}

	return cli.db.ZScore(args[0], 0, args[1])
}
