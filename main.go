package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/siddontang/goredis"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Println("redis2ledis <redis_host:port> <redis_db_idx> <ledis_host:port> <ledis_db_idx>")
		return
	}

	redisAddr, redisDBIdx := os.Args[1], os.Args[2]
	ledisAddr, ledisDBIdx := os.Args[3], os.Args[4]

	redis := goredis.NewClient(redisAddr, "")
	defer redis.Close()

	redisConn, err := redis.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer redisConn.Close()

	ledis := goredis.NewClient(ledisAddr, "")
	defer ledis.Close()

	ledisConn, err := ledis.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer ledisConn.Close()

	err = copyDB(redisConn, ledisConn, redisDBIdx, ledisDBIdx)
	if err != nil {
		log.Fatal(err)
	}
}

func copyDB(redisConn, ledisConn *goredis.PoolConn, redisDBIdx, ledisDBIdx string) error {
	ok, err := goredis.String(redisConn.Do("SELECT", redisDBIdx))
	if err != nil {
		return err
	}
	if ok != "OK" {
		return fmt.Errorf("Redis Select %s failed", redisDBIdx)
	}

	ok, err = goredis.String(ledisConn.Do("SELECT", ledisDBIdx))
	if err != nil {
		return err
	}
	if ok != "OK" {
		return fmt.Errorf("Redis Select %s failed", ledisDBIdx)
	}

	keys, err := goredis.Values(redisConn.Do("KEYS", "*"))
	if err != nil {
		return err
	}

	for _, key := range keys {
		err = copyKey(redisConn, ledisConn, key.([]byte))
		if err != nil {
			return err
		}
	}
	return nil
}

func copyKey(redisConn, ledisConn *goredis.PoolConn, key []byte) error {
	tp, err := goredis.String(redisConn.Do("TYPE", key))
	if err != nil {
		return err
	}

	switch tp {
	case "hash":
		return copyHash(redisConn, ledisConn, key)
	}

	log.Printf("unsupported %s type key %s", tp, string(key))
	return nil
}

func copyHash(redisConn, ledisConn *goredis.PoolConn, key []byte) error {
	kvs, err := redisConn.Do("HGETALL", key)
	if err != nil {
		return err
	}

	args := make([]interface{}, 0, len(kvs.([]interface{}))+1)
	args = append(args, key)
	args = append(args, kvs.([]interface{})...)
	ok, err := goredis.String(ledisConn.Do("HMSET", args...))
	if err != nil {
		return err
	}
	if ok != "OK" {
		return errors.New("copy data failed")
	}

	ttl, err := redisConn.Do("TTL", key)
	if err != nil {
		return err
	}
	if ttl.(int64) > 0 {
		status, err := goredis.Int(ledisConn.Do("HEXPIRE", key, ttl))
		if err != nil {
			return err
		}
		if status != 1 {
			return fmt.Errorf("set ttl for hash key %s failed", string(key))
		}
	}
	log.Printf("copied hash %s", string(key))
	return nil
}
