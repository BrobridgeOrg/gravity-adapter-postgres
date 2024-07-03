package parser

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ValueType int32

const (
	NormalValue ValueType = iota
	NullValue
)

var (
	InvalidErr = errors.New("Invalid Syntax")
)

type Parser struct {
	Operation string
	Table     string
	AfterData map[string]interface{}
}

func NewParser() *Parser {
	return &Parser{
		AfterData: make(map[string]interface{}),
	}
}

func (p *Parser) unescape(text string) string {
	text = strings.ReplaceAll(text, "\\\\", "\\")
	text = strings.ReplaceAll(text, "\\\"", "\"")
	text = strings.ReplaceAll(text, "\\,", ",")
	text = strings.ReplaceAll(text, "''", "'")
	return text
}

func (p *Parser) getValue(str string) (ValueType, string, string, error) {

	// Check quote character
	if str[0] == '\'' {
		v, cur, err := p.getRawString(str)
		if err != nil {
			return NormalValue, "", str, InvalidErr
		}

		return NormalValue, p.unescape(v), strings.TrimSpace(str[cur:]), nil
	}

	left := ""
	v := str

	if i := strings.IndexByte(str, ' '); i >= 0 {
		v = str[:i]
		left = strings.TrimSpace(str[i+1:])
	}

	if v == "null" {
		return NullValue, "", left, nil
	}

	return NormalValue, v, left, nil
}

func (p *Parser) getRawString(text string) (string, int, error) {

	if text[0] != '\'' {
		return "", 0, InvalidErr
	}

	cur := 1
	for {

		if len(text[cur:]) == 0 {
			return "", cur, InvalidErr
		}

		// Check quote character in string
		if i := strings.IndexByte(text[cur:], '\''); i >= 0 {

			if len(text) >= cur+i+2 {

				// It's quote character in string rather than a token
				if text[cur+i+1] == '\'' {
					cur += i + 2
					continue
				}
			}

			data := text[1 : cur+i]

			return data, cur + i + 1, nil
		}
	}
}

func (p *Parser) getString(text string) (string, int, error) {

	value, cur, err := p.getRawString(text)
	if err != nil {
		return value, cur, InvalidErr
	}

	return p.unescape(value), cur, nil
}

func (p *Parser) getArrayElementString(text string) (string, string, error) {

	if text[0] != '"' {
		return "", text, InvalidErr
	}

	cur := 1
	for {

		if len(text[cur:]) == 0 {
			return "", text[cur:], InvalidErr
		}

		// Check quote character in string
		if i := strings.IndexByte(text[cur:], '"'); i >= 0 {

			if cur > 0 {

				// It's quote character in string rather than a token
				if text[cur+i-1] == '\\' {

					cur += i + 1
					continue
				}
			}

			data := text[1 : cur+i]

			return p.unescape(data), text[cur+i+1:], nil
		}
	}
}

func (p *Parser) parseValue(valueType string, text string) (interface{}, error) {

	switch valueType {
	case "boolean":
		// Parse
		val, err := strconv.ParseBool(text)
		if err != nil {
			return "", err
		}

		return val, nil
	case "smallint":
		fallthrough
	case "bigint":
		fallthrough
	case "integer":

		// Parse
		val, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return "", err
		}

		return val, nil
	case "real":
		fallthrough
	case "numeric":
		// TODO: It might be longer
		fallthrough
	case "double precision":

		// Parse integer
		if i := strings.IndexByte(text, ' '); i >= 0 {
			v := text[:i]
			text = strings.TrimSpace(text[i+1:])

			// Parse
			val, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return "", err
			}

			return float64(val), nil
		}
	}

	return p.unescape(text), nil
}
func (p *Parser) parseArrayElement(valueType string, text string) (interface{}, string, error) {

	// It is array
	if text[0] == '{' {
		return p.parseArrayElements(valueType, text)
	}

	// Element is simple value
	if text[0] != '"' {

		source := text

		// Finding separator character
		for cur := 0; cur < len(source); cur++ {
			switch source[cur] {
			case ',':
				// End of this element

				// Parse content
				value, err := p.parseValue(valueType, source[:cur])
				if err != nil {
					return nil, text, err
				}

				return value, source[cur:], nil
			case '}':
				// End of array

				// Parse content
				value, err := p.parseValue(valueType, source[:cur])
				if err != nil {
					return nil, text, err
				}

				return value, source[cur:], nil
			}
		}

		return nil, source, errors.New("Invalid array element")
	}

	// Element is string
	value, source, err := p.getArrayElementString(text)
	if err != nil {
		return nil, source, errors.New("Invalid array element")
	}

	return value, source, nil
}

func (p *Parser) parseArrayElements(valueType string, text string) ([]interface{}, string, error) {

	source := text[1:]
	values := make([]interface{}, 0)

	// Parsing each element
	for len(source) > 0 {

		value, newSource, err := p.parseArrayElement(valueType, source)
		if err != nil {
			return nil, newSource, err
		}

		values = append(values, value)

		if len(newSource) == 0 {
			source = newSource
			break
		}

		switch newSource[0] {
		case ',':
			fallthrough
		case '}':
			source = newSource[1:]
			break
		}
	}

	return values, source, nil
}

func (p *Parser) parseArray(valueType string, text string) ([]interface{}, error) {

	if text[0] != '{' || text[len(text)-1] != '}' {
		return nil, fmt.Errorf("%v: Not valid array value", InvalidErr)
	}

	//source := text[1 : len(text)-1]

	value, source, err := p.parseArrayElements(valueType, text)
	if err != nil {
		return nil, fmt.Errorf("%v: %s\n", err, source)
	}

	return value, nil
}

func (p *Parser) parseField(text string) (string, error) {

	var fieldName string
	var fieldType string

	// Getting field name
	if i := strings.IndexByte(text, '['); i >= 0 {
		fieldName = text[:i]
		text = text[i+1:]
	}

	if len(text) == 0 {
		return "", InvalidErr
	}

	// Find separator
	if i := strings.IndexByte(text, ':'); i > 0 {
		if text[i-1] != ']' {
			return "", InvalidErr
		}

		// Getting data type
		fieldType = text[:i-1]
		text = text[i+1:]
	}

	if len(text) == 0 {
		return "", InvalidErr
	}

	// Check whether array type
	if strings.Contains(fieldType, "[]") {
		valueType := fieldType[:len(fieldType)-2]

		// Parse string
		str, cur, err := p.getRawString(text)
		if err != nil {
			return text, err
		}

		text = strings.TrimSpace(text[cur:])

		// Parse array string
		value, err := p.parseArray(valueType, str)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = value

		return text, nil
	}

	// Extract value
	vt, v, left, err := p.getValue(text)
	if err != nil {
		return left, err
	}

	if vt == NullValue {
		p.AfterData[fieldName] = nil
		return left, nil
	}

	// Getting value
	switch fieldType {
	case "boolean":

		// Parse
		val, err := strconv.ParseBool(v)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = val

	case "smallint":
		fallthrough
	case "bigint":
		fallthrough
	case "integer":

		// Parse
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = val

	case "real":
		fallthrough
	case "numeric":
		// TODO: It might be longer
		fallthrough
	case "double precision":

		// Parse
		val, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = val

	case "bytea":

		// Parse
		val, err := hex.DecodeString(v[2:])
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = val

	case "money":

		if v[0] == '$' {
			v = v[1:]
		}

		// Parse
		val, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = val

	case "timestamp without time zone":

		v = strings.ReplaceAll(v, " ", "T") + "Z"

		// Parse timestamp
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = t

	case "interval":
		fallthrough
	case "time without time zone":

		p.AfterData[fieldName] = v

	case "date":

		// Parse date
		t, _ := time.Parse("2006-01-02", v)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = t

	case "bit":
		fallthrough
	case "bit varying":

		if v[0] != 'B' {
			return text, InvalidErr
		}

		p.AfterData[fieldName] = v[2 : len(v)-1]

	default:
		p.AfterData[fieldName] = v
	}

	return left, nil
}

func (p *Parser) parseFields(text string) error {

	data := text
	for {
		t, err := p.parseField(data)
		if err != nil {
			return err
		}

		if len(t) == 0 {
			break
		}

		data = t
	}

	return nil
}

func (p *Parser) Parse(source string) error {

	if len(source) < 6 {
		return InvalidErr
	}

	if source[:6] != "table " {
		// Ignore
		return nil
	}

	text := source[6:]

	// Getting table name
	if i := strings.IndexByte(text, ':'); i >= 0 {
		p.Table = text[:i]
		text = strings.TrimSpace(text[i+1:])
	} else {
		return InvalidErr
	}

	if len(text) == 0 {
		return InvalidErr
	}

	// Getting operation
	if i := strings.IndexByte(text, ':'); i >= 0 {
		p.Operation = text[:i]
		text = strings.TrimSpace(text[i+1:])
	} else {
		return InvalidErr
	}

	if len(text) == 0 {
		return InvalidErr
	}

	// Parsing fields
	return p.parseFields(text)
}
