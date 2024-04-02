package mariadb

import (
	"github.com/channelonerussia/db"
	mdb "github.com/channelonerussia/mariadb"
)

type mdbClient struct {
	masterDBC db.DB
}

func New(host, port, username, password, database string) (db.Client, error) {

	db, err := mdb.New(host, port, username, password, database)

	if err != nil {
		return nil, err
	}

	return &mdbClient{
		masterDBC: NewDB(db),
	}, nil
}

func (c *mdbClient) DB() db.DB {
	return c.masterDBC
}

func (c *mdbClient) Close() error {
	if c.masterDBC != nil {
		c.masterDBC.Close()
	}

	return nil
}
