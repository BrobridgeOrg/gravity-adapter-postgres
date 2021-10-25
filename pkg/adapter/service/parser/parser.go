package parser

import (
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
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

func (p *Parser) getString(text string) (string, int, error) {

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
			data = strings.ReplaceAll(data, "\\\"", "\"")
			data = strings.ReplaceAll(data, "''", "'")

			return data, cur + i + 1, nil
		}
	}
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

	// Getting value
	switch fieldType {
	case "boolean":

		// Parse bool
		if i := strings.IndexByte(text, ' '); i >= 0 {
			v := text[:i]
			text = strings.TrimSpace(text[i+1:])

			// Parse
			val, err := strconv.ParseBool(v)
			if err != nil {
				return "", err
			}

			p.AfterData[fieldName] = val

			return text, nil
		}
	case "smallint":
		fallthrough
	case "bigint":
		fallthrough
	case "integer":

		// Parse integer
		if i := strings.IndexByte(text, ' '); i >= 0 {
			v := text[:i]
			text = strings.TrimSpace(text[i+1:])

			// Parse
			val, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return "", err
			}

			p.AfterData[fieldName] = val

			return text, nil
		}
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

			p.AfterData[fieldName] = float64(val)

			return text, nil
		}
	case "bytea":

		// Parse string
		str, cur, err := p.getString(text)
		if err != nil {
			return "", err
		}

		// Convert to bytes
		data, err := hex.DecodeString(str[3:])
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = data

		text = strings.TrimSpace(text[cur:])

		return text, nil
	case "money":

		// Parse string
		str, cur, err := p.getString(text)
		if err != nil {
			return "", err
		}

		// Skip dollar sign and parse
		val, err := strconv.ParseFloat(str[1:], 64)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = val

		text = strings.TrimSpace(text[cur:])

		return text, nil
	case "timestamp without time zone":

		// Parse string
		str, cur, err := p.getString(text)
		if err != nil {
			return "", err
		}

		str = strings.ReplaceAll(str, " ", "T") + "Z"

		// Parse timestamp
		t, _ := time.Parse(time.RFC3339Nano, str)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = t

		text = strings.TrimSpace(text[cur:])

		return text, nil

	case "interval":
		fallthrough
	case "time without time zone":

		// Parse string
		str, cur, err := p.getString(text)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = str

		text = strings.TrimSpace(text[cur:])

		return text, nil

	case "date":

		// Parse string
		str, cur, err := p.getString(text)
		if err != nil {
			return "", err
		}

		// Parse timestamp
		t, _ := time.Parse("2006-01-02", str)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = t

		text = strings.TrimSpace(text[cur:])

		return text, nil

	case "bit":
		fallthrough
	case "bit varying":

		if text[0] != 'B' {
			return text, InvalidErr
		}

		text = text[1:]

		// Parse string
		str, cur, err := p.getString(text)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = str

		text = strings.TrimSpace(text[cur:])

		return text, nil

	default:

		// Parse string
		str, cur, err := p.getString(text)
		if err != nil {
			return "", err
		}

		p.AfterData[fieldName] = str

		text = strings.TrimSpace(text[cur:])

		return text, nil
	}

	return "", nil
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
