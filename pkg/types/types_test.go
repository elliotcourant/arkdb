package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEncode(t *testing.T) {
	encode := func(ty Type, value interface{}) {
		v, err := Encode(ty, value)
		if !assert.NoError(t, err) {
			panic(err)
		}
		assert.NotEmpty(t, v)
	}

	encodeBad := func(ty Type, value interface{}) {
		v, err := Encode(ty, value)
		if !assert.Error(t, err) {
			panic("no error")
		}
		assert.Nil(t, v)
	}

	t.Run("TypeTiny", func(t *testing.T) {
		encode(TypeTiny, uint8(123))
		encode(TypeTiny, int8(124))

		encodeBad(TypeTiny, "test")
	})

	t.Run("TypeShort", func(t *testing.T) {
		encode(TypeShort, uint16(123))
		encode(TypeShort, int16(215))

		encodeBad(TypeShort, "test")
	})

	t.Run("TypeLong", func(t *testing.T) {
		encode(TypeLong, uint32(513252435))
		encode(TypeLong, int32(543216532))

		encodeBad(TypeLong, "test")
	})

	t.Run("TypeLonglong", func(t *testing.T) {
		encode(TypeLonglong, uint64(513252435))
		encode(TypeLonglong, int64(543216532))

		encodeBad(TypeLonglong, "test")
	})

	t.Run("TypeNull", func(t *testing.T) {
		encode(TypeNull, nil)
	})

	t.Run("TypeTimestamp|TypeDatetime", func(t *testing.T) {
		ts := []Type{
			TypeTimestamp,
			TypeDatetime,
		}
		for _, ty := range ts {
			encode(ty, uint64(time.Now().UnixNano()))
			encode(ty, int64(time.Now().UnixNano()))
			encode(ty, time.Now())
			encode(ty, time.Now().Format(ISO8601))

			encodeBad(ty, 12412.4531)
			encodeBad(ty, "hjkgdshgjklads")
		}
	})

	t.Run("TypeDate", func(t *testing.T) {
		encode(TypeDate, uint64(time.Now().UnixNano()))
		encode(TypeDate, int64(time.Now().UnixNano()))
		encode(TypeDate, time.Now())
		encode(TypeDate, time.Now().Format(ISO8601Date))

		encodeBad(TypeDate, 12412.4531)
		encodeBad(TypeDate, time.Now().Format(ISO8601))
	})

	t.Run("TypeVarchar|TypeVarString|TypeString", func(t *testing.T) {
		ts := []Type{
			TypeVarchar, TypeVarString, TypeString,
		}
		for _, ty := range ts {
			encode(ty, time.Now().UnixNano())
			encode(ty, time.Now())
			encode(ty, time.Now().Format(ISO8601))
			encode(ty, []byte("test"))
			encode(ty, true)
			encode(ty, false)
			encode(ty, nil)
			encode(ty, 12412.4531)
			encode(ty, "hjkgdshgjklads")
			encode(ty, time.Since(time.Now()))
			encodeBad(ty, complex64(1))
		}
	})

	t.Run("TypeBit", func(t *testing.T) {
		encode(TypeBit, "true")
		encode(TypeBit, "false")
		encode(TypeBit, true)
		encode(TypeBit, false)

		encodeBad(TypeBit, "afhjksaf")
		encodeBad(TypeBit, 86431)
	})
}
