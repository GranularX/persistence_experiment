package main

import (
	"fmt"
	"net/http"

	"github.com/dgraph-io/badger/v4"
	"github.com/gin-gonic/gin"
)

type KVS struct {
	db *badger.DB
}

func main() {
	kvs, err := OpenKVS("data")
	handle(err)
	defer kvs.Close()

	r := gin.Default()

	r.POST("/api/kvs", func(c *gin.Context) {
		var requestData struct {
			Key   string `json:"key" binding:"required"`
			Value string `json:"value" binding:"required"`
		}

		if err := c.ShouldBindJSON(&requestData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if err := kvs.Set([]byte(requestData.Key), []byte(requestData.Value)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Key-Value pair added successfully"})
	})

	r.GET("/api/kvs/:key", func(c *gin.Context) {
		key := c.Param("key")

		value, err := kvs.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"key": key, "value": string(value)})
	})

	r.DELETE("/api/kvs/:key", func(c *gin.Context) {
		key := c.Param("key")

		err := kvs.Delete([]byte(key))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Key deleted successfully"})
	})

	fmt.Println("Server starting already.....")
	if err := r.Run(":8080"); err != nil {
		panic(err)
	}
}

func OpenKVS(path string) (*KVS, error) {
	opts := badger.DefaultOptions(path)
	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &KVS{
		db: badgerDB,
	}, nil
}

func (kvs *KVS) Close() error {
	return kvs.db.Close()
}

func (kvs *KVS) Set(key, value []byte) error {
	return kvs.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, value); err != nil {
			return err
		}
		return nil
	})
}

func (kvs *KVS) Delete(key []byte) error {
	return kvs.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(key); err != nil {
			return err
		}
		return nil
	})
}

func (kvs *KVS) Get(key []byte) ([]byte, error) {
	var valCopy []byte
	err := kvs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			valCopy = append(valCopy, val...)
			return nil
		})
		return err
	})

	return valCopy, err
}

func handle(err error) {
	if err != nil {
		panic(err)
	}
}
