package types

import (
	"fmt"
	"github.com/elliotcourant/buffers"
	"strconv"
	"time"
)

const (
	ISO8601     = "2006-01-02T15:04:05-0700"
	ISO8601Date = "2006-01-02"
)

type Type uint8

const (
	TypeDecimal   Type = 0
	TypeTiny      Type = 1
	TypeShort     Type = 2
	TypeLong      Type = 3
	TypeFloat     Type = 4
	TypeDouble    Type = 5
	TypeNull      Type = 6
	TypeTimestamp Type = 7
	TypeLonglong  Type = 8
	TypeInt24     Type = 9
	TypeDate      Type = 10
	/* TypeDuration original name was TypeTime, renamed to TypeDuration to resolve the conflict with Go type Time.*/
	TypeDuration Type = 11
	TypeDatetime Type = 12
	TypeYear     Type = 13
	TypeNewDate  Type = 14
	TypeVarchar  Type = 15
	TypeBit      Type = 16

	TypeJSON       Type = 0xf5
	TypeNewDecimal Type = 0xf6
	TypeEnum       Type = 0xf7
	TypeSet        Type = 0xf8
	TypeTinyBlob   Type = 0xf9
	TypeMediumBlob Type = 0xfa
	TypeLongBlob   Type = 0xfb
	TypeBlob       Type = 0xfc
	TypeVarString  Type = 0xfd
	TypeString     Type = 0xfe
	TypeGeometry   Type = 0xff
)

func Encode(t Type, value interface{}) ([]byte, error) {
	buf := buffers.NewBytesBuffer()
	if value == nil {
		t = TypeNull
	}
	incompatible := fmt.Errorf("cannot convert %T to %s", value, t.String())
	switch t {
	case TypeTiny:
		switch v := value.(type) {
		case uint8:
			buf.AppendUint8(v)
		case int8:
			buf.AppendUint8(uint8(v))
		default:
			return nil, incompatible
		}
	case TypeShort:
		switch v := value.(type) {
		case uint16:
			buf.AppendUint16(v)
		case int16:
			buf.AppendUint16(uint16(v))
		default:
			return nil, incompatible
		}
	case TypeLong:
		switch v := value.(type) {
		case uint32:
			buf.AppendUint32(v)
		case int32:
			buf.AppendInt32(v)
		default:
			return nil, incompatible
		}
	case TypeLonglong:
		switch v := value.(type) {
		case int:
			buf.AppendInt64(int64(v))
		case uint64:
			buf.AppendUint64(v)
		case int64:
			buf.AppendInt64(v)
		default:
			return nil, incompatible
		}
	case TypeNull:
		buf.Append(nil...)
	case TypeTimestamp, TypeDatetime:
		switch v := value.(type) {
		case uint64:
			buf.AppendUint64(v)
		case int64:
			buf.AppendInt64(v)
		case time.Time:
			buf.AppendInt64(v.UnixNano())
		case string:
			tm, err := time.Parse(ISO8601, v)
			if err != nil {
				return nil, err
			}
			return Encode(t, tm)
		default:
			return nil, incompatible
		}
	case TypeDate:
		switch v := value.(type) {
		case uint64:
			return Encode(t, int64(v))
		case int64:
			return Encode(t, time.Unix(0, v))
		case string:
			tm, err := time.Parse(ISO8601Date, v)
			if err != nil {
				return nil, err
			}
			return Encode(t, tm)
		case time.Time:
			year, month, day := v.Date()
			return Encode(TypeTimestamp, time.Date(year, month, day, 0, 0, 0, 0, v.Location()))
		default:
			return nil, incompatible
		}
	case TypeVarchar, TypeVarString, TypeString, TypeBlob:
		switch v := value.(type) {
		case string:
			buf.AppendString(v)
		case []byte:
			buf.Append(v...)
		case uint8, uint16, uint32, uint64, int8, int16, int32, int64:
			return Encode(t, fmt.Sprintf("%d", v))
		case float32, float64:
			return Encode(t, fmt.Sprintf("%f", v))
		case bool:
			val := "true"
			if !v {
				val = "false"
			}
			return Encode(t, val)
		case time.Time:
			return Encode(t, v.Format(ISO8601))
		case time.Duration:
			return Encode(t, v.String())
		default:
			return nil, incompatible
		}
	case TypeBit:
		switch v := value.(type) {
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, err
			}
			return Encode(t, b)
		case bool:
			buf.AppendBool(v)
		default:
			return nil, incompatible
		}
	default:
		return nil, incompatible
	}
	return buf.Bytes(), nil
}
