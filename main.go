package mongo_cache

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Cache struct {
	Key   string
	Value string
}

const (
	NotFound  = "not found"
	Duplicate = "E11000 duplicate"
)

var (
	session    *mgo.Session
	collection *mgo.Collection
	err        error
)

func Initialize(connectionString, DBName, collectionName string) (*mgo.Collection, error) {
	session, err = mgo.Dial(connectionString)
	if err != nil {
		CloseSession()
		return collection, err
	}

	collection = session.DB(DBName).C(collectionName)
	return collection, nil
}

func Get(key string) (Cache, error) {
	result := Cache{}
	err = collection.Find(bson.M{"key": key}).One(&result)
	if err != nil {
		errorString := err.Error()
		if errorString != NotFound {
			CloseSession()
			return result, err
		}
	}

	return result, nil
}

func Set(key, value string) error {
	err = collection.Insert(&Cache{key, value})

	if err != nil {
		errorString := err.Error()
		if errorString[:len(Duplicate)] != Duplicate {
			CloseSession()
			return err
		}
	}
	return nil
}

func CloseSession() {
	session.Close()
}
