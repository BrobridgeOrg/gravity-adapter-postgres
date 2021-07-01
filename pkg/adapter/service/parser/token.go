package parser

import (
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
)

type Token struct {
	Type  TokenType
	Value string
	Err   error
}
type TokenType uint8

const (
	OpenParenTok = TokenType(iota + 1)
	CloseParenTok
	CommaTok
	AtomTok
	StringTok
	NumberTok
	OpTok
	LineCommentTok
	BlockCommentTok
	EndTok
	BracketTok
	ColonTok
)

func gettok(text string) (Token, string) {
	text = strings.TrimSpace(text)
	if len(text) == 0 {
		return Token{Type: EndTok}, ""
	}

	if len(text) > 5 {
		switch text[:5] {
		case "table":
			if i := strings.IndexByte(text[5:], ' '); i >= 0 {
				return Token{Type: StringTok, Value: text[5 : 5+i]}, text[5+i+1:]
			}
		}

	}

	if len(text) > 1 {
		switch text[:2] {
		case "--":
			if i := strings.Index(text, "\n"); i >= 2 {
				return Token{Type: LineCommentTok, Value: text[2:i]}, text[i+1:]
			}
		case "/*":
			if i := strings.Index(text, "*/"); i >= 2 {
				return Token{Type: BlockCommentTok, Value: text[2:i]}, text[i+2:]
			}
		case "||", ":=":
			return Token{Type: OpTok, Value: text[:2]}, text[2:]
		}
	}
	switch text[0] {
	case '[':
		if i := strings.Index(text, "]:"); i >= 2 {
			return Token{Type: BracketTok, Value: text[1:i]}, text[i+2:]
		}
	case '(':
		return Token{Type: OpenParenTok}, text[1:]
	case ')':
		return Token{Type: CloseParenTok}, text[1:]
	case ',':
		return Token{Type: CommaTok}, text[1:]
	case ':':
		return Token{Type: ColonTok}, text[1:]
	case '\'':
		if i := strings.IndexByte(text[1:], '\''); i >= 0 {
			return Token{Type: StringTok, Value: text[1 : 1+i]}, text[1+i+1:]
		}
	case ';':
		return Token{Type: EndTok}, text[1:]
	case '-', '+', '=', '*', '<', '>', '/':
		return Token{Type: OpTok, Value: text[:1]}, text[1:]
	}
	r, size := utf8.DecodeRuneInString(text)
	if isDigit(r) {
		var i int
		if i = strings.IndexFunc(text[size:], notDigitDot); i < 0 {
			i = len(text) - size
		}
		return Token{Type: NumberTok, Value: text[:size+i]}, text[size+i:]
	}
	if isBeginName(r) {
		var i int
		if i = strings.IndexFunc(text[size:], notInName); i < 0 {
			i = len(text) - size
		}
		return Token{Type: AtomTok, Value: text[:size+i]}, text[size+i:]
	}

	return Token{Type: AtomTok, Err: errors.Errorf("unknown : %q", text)}, ""
}

func isBeginName(r rune) bool {
	return r == '.' || r == '"' || 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || r == '_'
}
func isInName(r rune) bool    { return r == '.' || r == '"' || '0' <= r && r <= '9' || isBeginName(r) }
func notInName(r rune) bool   { return !isInName(r) }
func isDigit(r rune) bool     { return '0' <= r && r <= '9' }
func notDigitDot(r rune) bool { return !(r == '.' || isDigit(r)) }
