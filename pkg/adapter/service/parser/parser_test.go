package parser

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseCommit(t *testing.T) {

	source := `COMMIT 559 location:[48 47 49 53 51 50 56 70 48] xid:[53 53 57]`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}
}

func TestParseBegin(t *testing.T) {

	source := `BEGIN 559 location:[48 47 49 53 51 50 55 50 48] xid:[53 53 57]`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}
}

func TestParseTable(t *testing.T) {

	source := `table public.users: INSERT: id[integer]:1 name[character]:'aaaaaa ' email[character]:'bbbbbbbb ' btest[bytea]:'\\x013d7d16d7ad4fefb61bd95b765c8ceb'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "public.users", parser.Table)
}

func TestParseInsertOperation(t *testing.T) {

	source := `table public.users: INSERT: id[integer]:1 name[character]:'aaaaaa ' email[character]:'bbbbbbbb ' btest[bytea]:'\\x013d7d16d7ad4fefb61bd95b765c8ceb' nulltest[character]:null`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "INSERT", parser.Operation)
}

func TestParseUpdateOperation(t *testing.T) {

	source := `table public.users: UPDATE: id[integer]:3 name[character]:'ccccccc ' email[character varying]:'cccc' btest[bytea]:'\\x013d7d16d7ad4fefb61bd95b765c8ceb' smallint1[smallint]:1 float1[double precision]:2 real1[real]:3 numeric1[numeric]:4.1 bigint1[bigint]:5 decimal1[numeric]:6 double1[double precision]:7 bool1[boolean]:true smallserial1[smallint]:1 serial1[integer]:1 bigserial1[bigint]:1 money[money]:'$30.00'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "UPDATE", parser.Operation)
}

func TestParseDeleteOperation(t *testing.T) {

	source := `table public.users: DELETE: id[integer]:3`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "DELETE", parser.Operation)
}

func TestParseFields(t *testing.T) {

	source := `table public.users: INSERT: id[integer]:1 name[character]:'aaaaaa ' email[character]:'bbbbbbbb ' btest[bytea]:'\\x013d7d16d7ad4fefb61bd95b765c8ceb'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, int64(1), parser.AfterData["id"].(int64))
	assert.Equal(t, "aaaaaa ", parser.AfterData["name"].(string))
	assert.Equal(t, "bbbbbbbb ", parser.AfterData["email"].(string))

	bytesResult, _ := hex.DecodeString("013d7d16d7ad4fefb61bd95b765c8ceb")
	assert.Equal(t, bytesResult, parser.AfterData["btest"].([]byte))
}

func TestParseSingleQuote(t *testing.T) {

	source := `table public.users: INSERT: name[character]:'aaa''aaa'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "aaa'aaa", parser.AfterData["name"].(string))
}

func TestParseDoubleQuote(t *testing.T) {

	source := `table public.users: INSERT: name[character]:'aaa\"aaa'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, `aaa"aaa`, parser.AfterData["name"].(string))
}

func TestParseTypes(t *testing.T) {

	source := `table public.users: INSERT: id[integer]:3 btest[bytea]:'\\x013d7d16d7ad4fefb61bd95b765c8ceb' smallint1[smallint]:1 float1[double precision]:2 real1[real]:3 numeric1[numeric]:4.1 bigint1[bigint]:5 decimal1[numeric]:6 double1[double precision]:7 bool1[boolean]:true smallserial1[smallint]:1 serial1[integer]:1 bigserial1[bigint]:1 money[money]:'$30.00'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	// Bytes
	bytesResult, _ := hex.DecodeString("013d7d16d7ad4fefb61bd95b765c8ceb")
	assert.Equal(t, bytesResult, parser.AfterData["btest"].([]byte))

	// Integer
	assert.Equal(t, int64(1), parser.AfterData["smallint1"].(int64))
	assert.Equal(t, int64(5), parser.AfterData["bigint1"].(int64))
	assert.Equal(t, int64(3), parser.AfterData["id"].(int64))
	assert.Equal(t, int64(1), parser.AfterData["smallserial1"].(int64))
	assert.Equal(t, int64(1), parser.AfterData["serial1"].(int64))
	assert.Equal(t, int64(1), parser.AfterData["bigserial1"].(int64))

	// Float
	assert.Equal(t, float64(2), parser.AfterData["float1"].(float64))
	assert.Equal(t, float64(3), parser.AfterData["real1"].(float64))
	assert.Equal(t, float64(4.1), parser.AfterData["numeric1"].(float64))
	assert.Equal(t, float64(6), parser.AfterData["decimal1"].(float64))
	assert.Equal(t, float64(7), parser.AfterData["double1"].(float64))
	assert.Equal(t, float64(30.00), parser.AfterData["money"].(float64))

	// Boolean
	assert.Equal(t, true, parser.AfterData["bool1"].(bool))
}

func TestParseStringTypes(t *testing.T) {

	source := `table public.users: INSERT: text1[text]:'abc' name[character]:'a ' email[character varying]:'bbbb'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "a ", parser.AfterData["name"].(string))
	assert.Equal(t, "bbbb", parser.AfterData["email"].(string))
	assert.Equal(t, "abc", parser.AfterData["text1"].(string))
}

func TestParseBooleanTypes(t *testing.T) {

	source := `table public.users: INSERT: field1[boolean]:true field2[boolean]:true`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, true, parser.AfterData["field1"].(bool))
	assert.Equal(t, true, parser.AfterData["field2"].(bool))
}

func TestParseByteaTypes(t *testing.T) {

	source := `table public.users: INSERT: btest[bytea]:'\\x013d7d16d7ad4fefb61bd95b765c8ceb'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	bytesResult, _ := hex.DecodeString("013d7d16d7ad4fefb61bd95b765c8ceb")
	assert.Equal(t, bytesResult, parser.AfterData["btest"].([]byte))
}

func TestParseIntegerTypes(t *testing.T) {

	source := `table public.users: INSERT: id[integer]:3 smallint1[smallint]:1 bigint1[bigint]:5 smallserial1[smallint]:1 serial1[integer]:1 bigserial1[bigint]:1`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	// Integer
	assert.Equal(t, int64(1), parser.AfterData["smallint1"].(int64))
	assert.Equal(t, int64(5), parser.AfterData["bigint1"].(int64))
	assert.Equal(t, int64(3), parser.AfterData["id"].(int64))
	assert.Equal(t, int64(1), parser.AfterData["smallserial1"].(int64))
	assert.Equal(t, int64(1), parser.AfterData["serial1"].(int64))
	assert.Equal(t, int64(1), parser.AfterData["bigserial1"].(int64))
}

func TestParseFloatTypes(t *testing.T) {

	source := `table public.users: INSERT: float1[double precision]:2 real1[real]:3 numeric1[numeric]:4.1 decimal1[numeric]:6 money[money]:'$30.00' double1[double precision]:7`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	// Float
	assert.Equal(t, float64(2), parser.AfterData["float1"].(float64))
	assert.Equal(t, float64(3), parser.AfterData["real1"].(float64))
	assert.Equal(t, float64(4.1), parser.AfterData["numeric1"].(float64))
	assert.Equal(t, float64(6), parser.AfterData["decimal1"].(float64))
	assert.Equal(t, float64(7), parser.AfterData["double1"].(float64))
	assert.Equal(t, float64(30.00), parser.AfterData["money"].(float64))
}

func TestParseTimeTypes(t *testing.T) {

	source := `table public.users: INSERT: timestamp1[timestamp without time zone]:'2021-10-25 11:21:58.172505' date1[date]:'2021-10-25' time1[time without time zone]:'11:21:58.172505' interval1[interval]:'00:00:01'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	// String
	assert.Equal(t, int64(1635160918), parser.AfterData["timestamp1"].(time.Time).Unix())
	assert.Equal(t, int64(1635120000), parser.AfterData["date1"].(time.Time).Unix())
	assert.Equal(t, "11:21:58.172505", parser.AfterData["time1"].(string))
	assert.Equal(t, "00:00:01", parser.AfterData["interval1"].(string))
}

func TestParseLocationTypes(t *testing.T) {

	source := `table public.users: INSERT: point1[point]:'(1,2)' line1[line]:'{1,2,3}' lseg1[lseg]:'[(1,2),(3,4)]' box1[box]:'(3,4),(1,2)' path1[path]:'((1,2),(3,4),(5,6))' polygon1[polygon]:'((1,2),(3,4),(5,6))' circle1[circle]:'<(1,2),3>'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "(1,2)", parser.AfterData["point1"].(string))
	assert.Equal(t, "{1,2,3}", parser.AfterData["line1"].(string))
	assert.Equal(t, "[(1,2),(3,4)]", parser.AfterData["lseg1"].(string))
	assert.Equal(t, "(3,4),(1,2)", parser.AfterData["box1"].(string))
	assert.Equal(t, "((1,2),(3,4),(5,6))", parser.AfterData["path1"].(string))
	assert.Equal(t, "((1,2),(3,4),(5,6))", parser.AfterData["polygon1"].(string))
	assert.Equal(t, "<(1,2),3>", parser.AfterData["circle1"].(string))
}

func TestParseBitTypes(t *testing.T) {

	source := `table public.users: INSERT: bit1[bit]:B'1' varbit1[bit varying]:B'101'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "1", parser.AfterData["bit1"].(string))
	assert.Equal(t, "101", parser.AfterData["varbit1"].(string))
}

func TestParseArrayTypes(t *testing.T) {

	source := `table public.users: INSERT: json1[json]:'{"aa":"bb"}' intarr[integer[]]:'{1,2,3}' varchararr1[character varying[]]:'{meeting,lunch}' varchararr2[character varying[]]:'{"meeting","lunch","aaa\"aaa",{second,"xxx"},test}' varchararr3[character varying[]]:'{"aaa\,aaa",bbb}'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "{\"aa\":\"bb\"}", parser.AfterData["json1"].(string))

	assert.Equal(t, int64(1), parser.AfterData["intarr"].([]interface{})[0].(int64))
	assert.Equal(t, int64(2), parser.AfterData["intarr"].([]interface{})[1].(int64))
	assert.Equal(t, int64(3), parser.AfterData["intarr"].([]interface{})[2].(int64))

	assert.Equal(t, "meeting", parser.AfterData["varchararr1"].([]interface{})[0].(string))
	assert.Equal(t, "lunch", parser.AfterData["varchararr1"].([]interface{})[1].(string))

	assert.Equal(t, "meeting", parser.AfterData["varchararr2"].([]interface{})[0].(string))
	assert.Equal(t, "lunch", parser.AfterData["varchararr2"].([]interface{})[1].(string))
	assert.Equal(t, "aaa\"aaa", parser.AfterData["varchararr2"].([]interface{})[2].(string))

	assert.Equal(t, "second", parser.AfterData["varchararr2"].([]interface{})[3].([]interface{})[0].(string))

	assert.Equal(t, "aaa,aaa", parser.AfterData["varchararr3"].([]interface{})[0].(string))

}

func TestParseSpecialTypes(t *testing.T) {

	source := `table public.users: INSERT: uuid1[uuid]:'98a4f867-8dcd-4982-aa3a-14e1030bcd88' xml1[xml]:'<foo>abc</foo>' cidr1[cidr]:'192.168.0.0/16' inet1[inet]:'198.24.10.0/24' macaddr1[macaddr]:'00:00:00:00:00:02'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "98a4f867-8dcd-4982-aa3a-14e1030bcd88", parser.AfterData["uuid1"].(string))
	assert.Equal(t, "<foo>abc</foo>", parser.AfterData["xml1"].(string))
	assert.Equal(t, "198.24.10.0/24", parser.AfterData["inet1"].(string))
	assert.Equal(t, "00:00:00:00:00:02", parser.AfterData["macaddr1"].(string))
}

func TestParseRangeTypes(t *testing.T) {

	source := `table public.users: INSERT: int4range1[int4range]:'[2,10)' int8range1[int8range]:'[11,21)' numrange1[numrange]:'[20,30]' tsrange1[tsrange]:'[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")' tstzrange1[tstzrange]:'[\"2010-01-01 06:30:00+00\",\"2010-01-01 07:30:00+00\")' daterange1[daterange]:'empty'`

	parser := NewParser()

	err := parser.Parse(source)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, "[2,10)", parser.AfterData["int4range1"].(string))
	assert.Equal(t, "[11,21)", parser.AfterData["int8range1"].(string))
	assert.Equal(t, "[20,30]", parser.AfterData["numrange1"].(string))
	assert.Equal(t, "[\"2010-01-01 14:30:00\",\"2010-01-01 15:30:00\")", parser.AfterData["tsrange1"].(string))
	assert.Equal(t, "[\"2010-01-01 06:30:00+00\",\"2010-01-01 07:30:00+00\")", parser.AfterData["tstzrange1"].(string))
	assert.Equal(t, "empty", parser.AfterData["daterange1"].(string))
}
