package parser

import (
	"errors"
	"io"
	"strconv"
	"strings"
)

type Parser struct {
	sqlStr      string
	sqlStrState string
	token       Token
	Operation   string
	Table       string
	BeforeData  map[string]*Value
	AfterData   map[string]*Value
}

func NewParser() *Parser {
	return &Parser{
		AfterData:  make(map[string]*Value),
		BeforeData: make(map[string]*Value),
	}
}

func (p *Parser) nextToken() Token {

	if p.sqlStrState == "" {
		return Token{Err: io.EOF}
	}

	tok, rem := gettok(p.sqlStrState)
	p.sqlStrState = rem
	p.token = tok

	return tok
}

func (p *Parser) getValue(tok Token) (*Value, error) {

	value := NewValue()

	switch tok.Type {
	case AtomTok:

		// It is NULL
		v := strings.ToUpper(tok.Value)
		if v == "NULL" {
			value.Data = nil
			break
		}

		value.Data = tok.Value

	case NumberTok:
		value.Data = tok.Value
		if val, err := strconv.ParseFloat(tok.Value, 64); err == nil {
			value.Data = val
		}
	case StringTok:
		value.Data = tok.Value
	default:
		return nil, errors.New("syntax error")
	}

	return value, nil
}

func (p *Parser) handleStatement() error {

	// Getting fields
	var allData []*Value
	for {

		if p.sqlStrState == "" {

			break
		}

		tok := p.nextToken()
		if tok.Err == io.EOF {
			return errors.New("syntax error")
		}

		if tok.Type == EndTok {
			break
		}

		if tok.Type == CloseParenTok {
			break
		}

		if tok.Type == CommaTok || tok.Type == BracketTok || tok.Type == ColonTok {
			continue
		}

		value, err := p.getValue(tok)
		if err != nil {
			return err
		}

		allData = append(allData, value)

	}

	afterData := make(map[string]*Value, len(allData)/2)
	tmpField := ""
	for i, data := range allData {
		if i%2 == 0 {
			tmpField = data.Data.(string)
		} else {
			afterData[tmpField] = data
		}
	}

	p.AfterData = afterData

	return nil
}

func (p *Parser) Parse(sqlStr string) error {

	p.sqlStr = sqlStr
	p.sqlStrState = sqlStr

	// table public.users: INSERT: id[integer]:19 name[character varying]:'ddddd' email[character varying]:'eeeeee'
	tok := p.nextToken()

	if tok.Err == io.EOF {
		return nil
	}

	if tok.Err != nil {
		return tok.Err
	}

	// Getting table name
	// public.users: INSERT: id[integer]:19 name[character varying]:'ddddd' email[character varying]:'eeeeee'
	tok = p.nextToken()

	if tok.Err == io.EOF {
		return nil
	}

	if tok.Err != nil {
		return tok.Err
	}

	if tok.Type == AtomTok {
		p.Table = tok.Value
	}

	// : INSERT: id[integer]:19 name[character varying]:'ddddd' email[character varying]:'eeeeee'
	tok = p.nextToken()

	if tok.Err == io.EOF {
		return nil
	}

	if tok.Err != nil {
		return tok.Err
	}

	// INSERT: id[integer]:19 name[character varying]:'ddddd' email[character varying]:'eeeeee'
	tok = p.nextToken()

	if tok.Err == io.EOF {
		return nil
	}

	if tok.Err != nil {
		return tok.Err
	}

	if tok.Type == AtomTok {
		value := strings.ToUpper(tok.Value)
		switch value {
		case "INSERT":
			p.Operation = "INSERT"
			return p.handleStatement()
		case "UPDATE":
			p.Operation = "UPDATE"
			return p.handleStatement()
		case "DELETE":
			p.Operation = "Delete"
			return p.handleStatement()
		}
	}

	return nil
}
