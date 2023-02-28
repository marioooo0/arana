/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hint

import (
	"bufio"
	"bytes"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/misc"
)

const (
	_            Type = iota
	TypeMaster        // force route to master node
	TypeSlave         // force route to slave node
	TypeRoute         // custom route
	TypeFullScan      // enable full-scan
	TypeDirect        // direct route
	TypeTrace         // distributed tracing
)

var _hintTypes = [...]string{
	TypeMaster:   "MASTER",
	TypeSlave:    "SLAVE",
	TypeRoute:    "ROUTE",
	TypeFullScan: "FULLSCAN",
	TypeDirect:   "DIRECT",
	TypeTrace:    "TRACE",
}

// KeyValue represents a pair of key and value.
type KeyValue struct {
	K string // key (optional)
	V string // value
}

// Type represents the type of Hint.
type Type uint8

// String returns the display string.
func (tp Type) String() string {
	return _hintTypes[tp]
}

// Hint represents a Hint, a valid Hint should include type and input kv pairs.
//
// Follow the format below:
//   - without inputs: YOUR_HINT()
//   - with non-keyed inputs: YOUR_HINT(foo,bar,quz)
//   - with keyed inputs: YOUR_HINT(x=foo,y=bar,z=quz)
type Hint struct {
	Type   Type
	Inputs []KeyValue
}

// String returns the display string.
func (h Hint) String() string {
	var sb strings.Builder
	sb.WriteString(h.Type.String())

	if len(h.Inputs) < 1 {
		sb.WriteString("()")
		return sb.String()
	}

	sb.WriteByte('(')

	writeKv := func(p KeyValue) {
		if key := p.K; len(key) > 0 {
			sb.WriteString(key)
			sb.WriteByte('=')
		}
		sb.WriteString(p.V)
	}

	writeKv(h.Inputs[0])
	for i := 1; i < len(h.Inputs); i++ {
		sb.WriteByte(',')
		writeKv(h.Inputs[i])
	}

	sb.WriteByte(')')
	return sb.String()
}

// Parse parses Hint from an input string.
func Parse(s string) (*Hint, error) {
	var (
		tpStr string
		tp    Type
	)

	offset := strings.Index(s, "(")
	if offset == -1 {
		tpStr = s
	} else {
		tpStr = s[:offset] //开头到(之前的内容
	}

	for i, v := range _hintTypes {
		if strings.EqualFold(tpStr, v) { //判断两个utf-8编码字符串（将unicode大写、小写、标题三种格式字符视为相同）是否相同
			tp = Type(i)
			break
		}
	}

	if tp == 0 {
		return nil, errors.Errorf("hint: invalid input '%s'", s)
	}

	if offset == -1 {
		return &Hint{Type: tp}, nil
	}

	end := strings.LastIndex(s, ")")
	if end == -1 {
		return nil, errors.Errorf("hint: invalid input '%s'", s)
	}

	s = s[offset+1 : end] //括号里面的内容

	scanner := bufio.NewScanner(strings.NewReader(s)) //为什么不直接用strings.Split?
	scanner.Split(scanComma)

	var kvs []KeyValue

	for scanner.Scan() {
		text := scanner.Text() //scan与text配合，按split分割拿到每个元素，直到结束

		// split kv by '='
		i := strings.Index(text, "=")
		if i == -1 {
			// omit blank text
			if misc.IsBlank(text) {
				continue
			}
			kvs = append(kvs, KeyValue{V: strings.TrimSpace(text)}) //没有任何=，那么k=空，v=去掉空格后text
		} else {
			var (
				k = strings.TrimSpace(text[:i])
				v = strings.TrimSpace(text[i+1:])
			)
			// omit blank key/value
			if misc.IsBlank(k) || misc.IsBlank(v) {
				continue
			}
			kvs = append(kvs, KeyValue{K: k, V: v}) //按照=拆分kv
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "hint: invalid input '%s'", s)
	}

	return &Hint{Type: tp, Inputs: kvs}, nil
}

func scanComma(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ','); i >= 0 {
		return i + 1, data[0:i], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

func Contains(hType Type, hints []*Hint) bool {
	for _, v := range hints {
		if v.Type == hType {
			return true
		}
	}
	return false
}
