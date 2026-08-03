package redis

import "errors"

func (rds *RedisDataStruct) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStruct) Type(key []byte) (redisDataType, error) {
	res, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(res) == 0 {
		return 0, errors.New("value is null")
	}
	return res[0], nil
}
