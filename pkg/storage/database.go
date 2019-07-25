package storage

type Database struct {
	DatabaseID   uint8
	DatabaseName string
}

func (d *Database) Path() []byte {
	return nil
}
