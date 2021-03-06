package server

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/r0123r/vredis/ledis"
)

func cmd_Command(c *client) error {
	return ErrCmdParams
}

func cmd_Object(c *client) error {
	return ErrCmdParams
}
func cmd_TTL(c *client) error {
	args := c.args
	if len(args) != 1 {
		return ErrCmdParams
	}
	key := args[0]
	ret := int64(-2)
	if ok, _ := c.db.LKeyExists(key); ok == 1 {
		ret, _ = c.db.LTTL(key)
	} else if ok, _ := c.db.SKeyExists(key); ok == 1 {
		ret, _ = c.db.STTL(key)
	} else if ok, _ := c.db.ZKeyExists(key); ok == 1 {
		ret, _ = c.db.ZTTL(key)
	} else if ok, _ := c.db.HKeyExists(key); ok == 1 {
		ret, _ = c.db.HTTL(key)
	} else if ok, _ := c.db.Exists(key); ok == 1 {
		ret, _ = c.db.TTL(key)
	}
	c.resp.writeInteger(ret)
	return nil
}

func cmd_Type(c *client) error {
	args := c.args
	if len(args) != 1 {
		return ErrCmdParams
	}
	k := args[0]

	if exists, _ := c.db.Exists(k); exists == 1 {
		c.resp.writeStatus("string")
	} else if exists, _ := c.db.HKeyExists(k); exists == 1 {
		c.resp.writeStatus("hash")
	} else if exists, _ := c.db.LKeyExists(k); exists == 1 {
		c.resp.writeStatus("list")
	} else if exists, _ := c.db.SKeyExists(k); exists == 1 {
		c.resp.writeStatus("set")
	} else if exists, _ := c.db.ZKeyExists(k); exists == 1 {
		c.resp.writeStatus("zset")
	} else {
		c.resp.writeStatus("none")
	}
	return nil
}
func cmd_FlushAll(c *client) error {
	for i := 0; i < c.app.cfg.Databases; i++ {
		db, err := c.ldb.Select(i)
		if err != nil {
			return err
		}
		db.FlushAll()
	}
	c.resp.writeStatus(OK)
	return nil
}
func cmd_FlushDB(c *client) error {
	if _, err := c.db.FlushAll(); err != nil {
		return err
	}
	c.resp.writeStatus(OK)
	return nil
}
func cmd_Del(c *client) error {
	if len(c.args) == 0 {
		return ErrCmdParams
	}
	count := int64(0)
	for _, k := range c.args {
		if exists, _ := c.db.Exists(k); exists == 1 {
			c.db.Del(k)
			count++
		}
		if exists, _ := c.db.HKeyExists(k); exists == 1 {
			c.db.HClear(k)
			count++
		}

		if exists, _ := c.db.LKeyExists(k); exists == 1 {
			c.db.LClear(k)
			count++
		}
		if exists, _ := c.db.SKeyExists(k); exists == 1 {
			c.db.SClear(k)
			count++
		}
		if exists, _ := c.db.ZKeyExists(k); exists == 1 {
			c.db.ZClear(k)
			count++
		}
	}
	c.resp.writeInteger(count)
	return nil
}
func cmd_Exists(c *client) error {
	if len(c.args) == 0 {
		return ErrCmdParams
	}
	count := int64(0)
	for _, k := range c.args {
		if exists, _ := c.db.Exists(k); exists == 1 {
			count++
		}
		if exists, _ := c.db.HKeyExists(k); exists == 1 {
			count++
		}

		if exists, _ := c.db.LKeyExists(k); exists == 1 {
			count++
		}
		if exists, _ := c.db.SKeyExists(k); exists == 1 {
			count++
		}
		if exists, _ := c.db.ZKeyExists(k); exists == 1 {
			count++
		}
	}
	c.resp.writeInteger(count)
	return nil
}

func patternRE(k string) string {
	re := bytes.Buffer{}
	re.WriteString(`^\Q`)
	for i := 0; i < len(k); i++ {
		p := k[i]
		switch p {
		case '*':
			re.WriteString(`\E.*\Q`)
		case '?':
			re.WriteString(`\E.\Q`)
		case '[':
			charClass := bytes.Buffer{}
			i++
			for ; i < len(k); i++ {
				if k[i] == ']' {
					break
				}
				if k[i] == '\\' {
					if i == len(k)-1 {
						// Ends with a '\'. U-huh.
						return ""
					}
					charClass.WriteByte(k[i])
					i++
					charClass.WriteByte(k[i])
					continue
				}
				charClass.WriteByte(k[i])
			}
			if charClass.Len() == 0 {
				// '[]' is valid in Redis, but matches nothing.
				return ""
			}
			re.WriteString(`\E[`)
			re.Write(charClass.Bytes())
			re.WriteString(`]\Q`)

		case '\\':
			if i == len(k)-1 {
				// Ends with a '\'. U-huh.
				return ""
			}
			// Forget the \, keep the next char.
			i++
			re.WriteByte(k[i])
			continue
		default:
			re.WriteByte(p)
		}
	}
	re.WriteString(`\E$`)
	return re.String()
}
func cmd_Keys(c *client) error {
	var err error
	if len(c.args) != 1 {
		return ErrCmdParams
	}

	var values, val [][]byte
	match := patternRE(string(c.args[0]))
	cursor := []byte{}
	count := 10
	for {
		val, err = c.db.Scan(ledis.KV, cursor, count, false, match)
		if err != nil {
			return err
		}
		values = append(values, val...)
		if len(val) < count {
			cursor = []byte{}
			break
		}
		cursor = val[len(val)-1]
	}
	for {
		val, err = c.db.Scan(ledis.LIST, cursor, count, false, match)
		if err != nil {
			return err
		}
		values = append(values, val...)
		if len(val) < count {
			cursor = []byte{}
			break
		}
		cursor = val[len(val)-1]
	}
	for {
		val, err = c.db.Scan(ledis.SET, cursor, count, false, match)
		if err != nil {
			return err
		}
		values = append(values, val...)

		if len(val) < count {
			cursor = []byte{}
			break
		}
		cursor = val[len(val)-1]
	}
	for {
		val, err = c.db.Scan(ledis.ZSET, cursor, count, false, match)
		if err != nil {
			return err
		}
		values = append(values, val...)

		if len(val) < count {
			cursor = []byte{}
			break
		}
		cursor = val[len(val)-1]
	}
	for {
		val, err = c.db.Scan(ledis.HASH, cursor, count, false, match)
		if err != nil {
			return err
		}
		values = append(values, val...)

		if len(val) < count {
			cursor = []byte{}
			break
		}
		cursor = val[len(val)-1]
	}

	c.resp.writeSliceArray(values)
	return nil
}
func cmd_Scan(c *client) error {
	// MATCH and COUNT options
	var err error
	pattern := "*"
	count := int(-1)
	if len(c.args) < 1 {
		return ErrCmdParams
	}

	cursor := c.args[0]
	args := c.args[1:]
	for len(args) > 1 {
		if bytes.EqualFold(args[0], []byte("count")) {
			// we do nothing with count
			if len(args) < 2 {
				return ErrSyntax
			}
			count, err = strconv.Atoi(string(args[1]))
			if err != nil {
				return err
			}
			args = args[2:]
			continue
		}
		if bytes.EqualFold(args[0], []byte("match")) {
			if len(args) < 2 {
				return ErrSyntax
			}
			pattern, args = string(args[1]), args[2:]
			continue
		}
		return ErrSyntax
	}

	var values, val [][]byte
	match := patternRE(pattern)
	values, err = c.db.Scan(ledis.KV, cursor, count, false, match)
	if err != nil {
		return err
	}
	val, err = c.db.Scan(ledis.LIST, cursor, count, false, match)
	if err != nil {
		return err
	}
	values = append(values, val...)
	val, err = c.db.Scan(ledis.SET, cursor, count, false, match)
	if err != nil {
		return err
	}
	values = append(values, val...)
	val, err = c.db.Scan(ledis.ZSET, cursor, count, false, match)
	if err != nil {
		return err
	}
	values = append(values, val...)
	val, err = c.db.Scan(ledis.HASH, cursor, count, false, match)
	if err != nil {
		return err
	}
	values = append(values, val...)
	rez := make([]interface{}, len(values))
	for i, v := range values {
		rez[i] = v
	}
	ret := []interface{}{cursor, rez}
	c.resp.writeArray(ret)
	return nil
}
func cmd_Rename(c *client) error {
	//oldKey, newkey []byte) (string, error) {
	if len(c.args) < 2 {
		return ErrCmdParams
	}
	oldKey := c.args[0]
	newKey := c.args[1]
	ttl := int64(-2)
	var val []byte
	if exists, _ := c.db.Exists(oldKey); exists == 1 {
		val, _ = c.db.Dump(oldKey)
		ttl, _ = c.db.TTL(oldKey)
		c.db.Del(oldKey)
	} else if exists, _ := c.db.HKeyExists(oldKey); exists == 1 {
		val, _ = c.db.HDump(oldKey)
		ttl, _ = c.db.HTTL(oldKey)
		c.db.HClear(oldKey)
	} else if exists, _ := c.db.LKeyExists(oldKey); exists == 1 {
		val, _ = c.db.LDump(oldKey)
		ttl, _ = c.db.LTTL(oldKey)
		c.db.LClear(oldKey)
	} else if exists, _ := c.db.SKeyExists(oldKey); exists == 1 {
		val, _ = c.db.SDump(oldKey)
		ttl, _ = c.db.STTL(oldKey)
		c.db.SClear(oldKey)
	} else if exists, _ := c.db.ZKeyExists(oldKey); exists == 1 {
		val, _ = c.db.ZDump(oldKey)
		ttl, _ = c.db.ZTTL(oldKey)
		c.db.ZClear(oldKey)
	} else {
		return nil
	}

	err := c.db.Restore(newKey, ttl, val)
	if err != nil {
		return err
	}
	c.resp.writeStatus(OK)
	return nil
}
func cmd_DbSize(c *client) error {

	count := int(0)
	cursor := []byte{}
	var keys [][]byte
	for {
		keys, _ = c.db.Scan(ledis.KV, cursor, 100, false, ".*")
		count += len(keys)
		if len(keys) < 100 {
			break
		}
		cursor = keys[len(keys)-1]
	}
	cursor = []byte{}
	for {
		keys, _ = c.db.Scan(ledis.LIST, cursor, 100, false, ".*")
		count += len(keys)
		if len(keys) < 100 {
			break
		}
		cursor = keys[len(keys)-1]
	}
	cursor = []byte{}
	for {
		keys, _ = c.db.Scan(ledis.SET, cursor, 100, false, ".*")
		count += len(keys)
		if len(keys) < 100 {
			break
		}
		cursor = keys[len(keys)-1]
	}
	cursor = []byte{}
	for {
		keys, _ = c.db.Scan(ledis.ZSET, cursor, 100, false, ".*")
		count += len(keys)
		if len(keys) < 100 {
			break
		}
		cursor = keys[len(keys)-1]
	}
	cursor = []byte{}
	for {
		keys, _ = c.db.Scan(ledis.HASH, cursor, 100, false, ".*")
		count += len(keys)
		if len(keys) < 100 {
			break
		}
		cursor = keys[len(keys)-1]
	}
	c.resp.writeInteger(int64(count))
	return nil
}
func cmd_HExists(c *client) error {
	//(key, field []byte) (int, error) {
	if len(c.args) != 2 {
		return ErrCmdParams
	}
	key := c.args[0]
	field := c.args[1]
	ret, err := c.db.HKeys(key)
	if err != nil {
		return err
	}
	for _, k := range ret {
		if bytes.Equal(k, field) {
			c.resp.writeInteger(1)
			return nil
		}
	}
	return nil
}
func cmd_SRem(c *client) error {
	//(key []byte, fields ...[]byte) (int64, error) {
	if len(c.args) < 2 {
		return ErrCmdParams
	}
	key := c.args[0]
	fields := c.args[1:]
	ff := [][]byte{}
	for _, f := range fields {
		if len(f) > 0 {
			ff = append(ff, f)
		}
	}
	ret, err := c.db.SRem(key, ff...)
	if err != nil {
		return err
	}
	c.resp.writeInteger(ret)
	return nil
}
func cmd_ZRem(c *client) error {
	//(key []byte, fields ...[]byte) (int64, error) {
	if len(c.args) < 2 {
		return ErrCmdParams
	}
	key := c.args[0]
	fields := c.args[1:]
	ff := [][]byte{}
	for _, f := range fields {
		if len(f) > 0 {
			ff = append(ff, f)
		}
	}
	ret, err := c.db.ZRem(key, ff...)
	if err != nil {
		return err
	}
	c.resp.writeInteger(ret)
	return nil
}
func init() {
	register("command", cmd_Command)
	//	register("object", objectCommand)
	register("type", cmd_Type)
	register("ttl", cmd_TTL)
	register("del", cmd_Del)
	register("flushdb", cmd_FlushDB)
	register("flushall", cmd_FlushAll)
	register("rename", cmd_Rename)
	register("scan", cmd_Scan)
	register("dbsize", cmd_DbSize)
	//register("hexists", cmd_HExists)
	register("zrem", cmd_ZRem)
	register("srem", cmd_SRem)
	register("exists", cmd_Exists)
	register("expire", cmd_Expire)
	register("lrem", cmd_LRem)
	register("lset", cmd_LSet)
	register("publish", cmd_Publish)
	register("set", cmd_Set)
	register("keys", cmd_Keys)
	register("dump", cmd_Dump)
}
func cmd_Expire(c *client) error {
	args := c.args
	if len(args) != 2 {
		return ErrCmdParams
	}
	key := c.args[0]
	ret := int64(-2)
	duration, err := ledis.StrInt64(args[1], nil)
	if err != nil {
		return ErrValue
	}
	if ok, _ := c.db.Exists(key); ok == 1 {
		ret, _ = c.db.Expire(key, duration)
	} else if ok, _ := c.db.LKeyExists(key); ok == 1 {
		ret, _ = c.db.LExpire(key, duration)
	} else if ok, _ := c.db.SKeyExists(key); ok == 1 {
		ret, _ = c.db.SExpire(key, duration)
	} else if ok, _ := c.db.ZKeyExists(key); ok == 1 {
		ret, _ = c.db.ZExpire(key, duration)
	} else if ok, _ := c.db.HKeyExists(key); ok == 1 {
		ret, _ = c.db.HExpire(key, duration)
	}
	c.resp.writeInteger(ret)

	return nil
}

func cmd_LRem(c *client) error {
	//(key []byte, count int, val []byte) (int, error) {
	if len(c.args) < 3 {
		return ErrCmdParams
	}
	key := c.args[0]
	//	count, err := ledis.StrInt64(c.args[1], nil)
	//	if err != nil {
	//		return err
	//	}
	val := c.args[2]
	stack, err := c.db.LRange(key, 0, -1)
	if err != nil {
		return err
	}
	b := stack[:0]
	k := int64(0)
	for _, x := range stack {
		if bytes.Equal(x, val) {
			k++
		} else {
			b = append(b, x)
		}
		//		if count == k {
		//			break
		//		}
	}
	if k > 0 {
		stack = stack[:len(stack)-int(k)]
	}
	c.db.LClear(key)
	c.db.LPush(key, stack...)
	c.resp.writeInteger(k)
	return nil
}
func cmd_LSet(c *client) error {
	//(key []byte, index int, value []byte) error {
	if len(c.args) < 3 {
		return ErrCmdParams
	}
	key := c.args[0]
	index, err := ledis.StrInt64(c.args[1], nil)
	if err != nil {
		return err
	}
	val := c.args[2]
	err = c.db.LSet(key, int32(index), val)
	if err != nil {
		return err
	}
	c.resp.writeStatus(OK)
	return nil
}

func cmd_Publish(c *client) error {
	//(key string, value []byte) (int, error) {
	if len(c.args) != 2 {
		return ErrCmdParams
	}
	c.app.rcm.Lock()
	i := Publish(c.app, c.args[0], c.args[1])
	c.app.rcm.Unlock()
	c.resp.writeInteger(i)
	return nil
}
func Publish(app *App, key, value []byte) int64 {
	i := int64(0)
	reply := []interface{}{[]byte("message"), key, value}
	for rc, _ := range app.rcs {
		if _, ok := rc.sub[string(key)]; ok {
			i++

			rc.resp.writeArray(reply)
			rc.resp.flush()

		}
	}
	return i
}
func cmd_Set(c *client) error {
	args := c.args

	if err := c.db.Set(args[0], args[1]); err != nil {
		return err
	} else {
		if len(args) > 1 {

			switch string(args[1]) {
			case "ex", "EX":
				expire, err := ledis.StrInt64(args[2], nil)
				if err != nil {
					return err
				}
				c.db.Expire(args[0], expire)
			}
		}
		c.resp.writeStatus(OK)
		notify := []byte(fmt.Sprint("__keyspace@", c.db.Index(), "__:", string(args[0])))

		go func(msg []byte) {
			c.app.rcm.Lock()
			defer c.app.rcm.Unlock()
			Publish(c.app, msg, []byte("set"))
		}(notify)
	}

	return nil
}

func cmd_Dump(c *client) error {
	if len(c.args) != 1 {
		return ErrCmdParams
	}
	var data []byte
	var err error
	key := c.args[0]
	if ok, _ := c.db.Exists(key); ok == 1 {
		data, err = c.db.Dump(key)
	} else if ok, _ := c.db.LKeyExists(key); ok == 1 {
		data, err = c.db.LDump(key)
	} else if ok, _ := c.db.SKeyExists(key); ok == 1 {
		data, err = c.db.SDump(key)
	} else if ok, _ := c.db.ZKeyExists(key); ok == 1 {
		data, err = c.db.ZDump(key)
	} else if ok, _ := c.db.HKeyExists(key); ok == 1 {
		data, err = c.db.HDump(key)
	} else {
		return ErrNotFound
	}
	if err != nil {
		return err
	} else {
		c.resp.writeBulk(data)
	}

	return nil
}
