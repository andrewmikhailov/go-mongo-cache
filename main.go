package go_mongo_cache

import (
	"log"

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

func Initialize(commectionString, collectionName string) {
	session, err = mgo.Dial(commectionString)
	if err != nil {
		CloseSession()
		panic(err)
	}

	collection = session.DB(collectionName).C("cache")

	index := mgo.Index{
		Key:    []string{"Key"},
		Unique: true,
	}
	err := collection.EnsureIndex(index)

	if err != nil {
		CloseSession()
		panic(err)
	}
}

func Get(key string) Cache {
	result := Cache{}
	err = collection.Find(bson.M{"key": key}).One(&result)
	if err != nil {
		errorString := err.Error()
		if errorString != NotFound {
			CloseSession()
			log.Fatal(err)
		}
	}

	return result
}

func Set(key, value string) {
	err = collection.Insert(&Cache{key, value})

	if err != nil {
		errorString := err.Error()
		if errorString[:len(Duplicate)] != Duplicate {
			CloseSession()
			log.Fatal(err)
		}
	}
}

func CloseSession() {
	session.Close()
}
