package postgres

import (
	sql_driver "database/sql/driver"
	"errors"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/pkg/providers/postgres/sqltimestamp"
)

type Date struct {
	pgtype.Date
}

var _ TextDecoderAndValuerWithHomo = (*Date)(nil)

// NewDate constructs a DATE representation which supports BC years
//
// TODO: remove this when https://st.yandex-team.ru/TM-5127 is done
func NewDate() *Date {
	return &Date{
		Date: *new(pgtype.Date),
	}
}

func (t *Date) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	if err := t.Date.DecodeText(ci, src); err != nil {
		tim, errF := sqltimestamp.Parse(string(src))
		infmod := isTimestampInfinite(string(src))
		if errF != nil && infmod == pgtype.None {
			return errors.Join(err, errF)
		}
		t.Date = pgtype.Date{Time: tim, Status: pgtype.Present, InfinityModifier: infmod}
	}

	return nil
}

func (t *Date) Value() (sql_driver.Value, error) {
	return t.Date.Value()
}

func (t *Date) HomoValue() any {
	switch t.Date.Status {
	case pgtype.Null:
		return nil
	case pgtype.Undefined:
		return nil
	}
	return t.Date
}
