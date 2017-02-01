package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	flag "github.com/jessevdk/go-flags"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Options struct {
	Host        string  `long:"host" default:"10.0.0.254" description:"mongodb host"`
	DBName      string  `long:"db" default:"mbench" description:"test db name"`
	Collection  string  `long:"collection" default:"mbench" description:"collection"`
	Mix         float64 `long:"mix" default:"0.5" description:"fraction of writes"`
	Concurrency int64   `long:"concurrency" default:"64" description:"concurrency"`
	Ops         int64   `long:"ops" default:"100000" description:"maximum ops"`
	Silent      bool    `long:"silent" description:"Suppress stdout"`
}

func main() {
	options := parseFlags()
	run(options)
}

func parseFlags() Options {
	var options Options
	flagParser := flag.NewParser(&options, flag.Default)
	_, err := flagParser.Parse()
	if err != nil {
		fmt.Println("Error parsing options", err)
		os.Exit(1)
	}
	return options
}

func genDoc(key int64) map[string]interface{} {
	doc := map[string]interface{}{
		"key":   key,
		"name":  "Test",
		"data":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"count": 123,
	}
	return doc
}

func newConn(options Options) (*mgo.Collection, error) {
	s, err := mgo.Dial(options.Host)
	if err != nil {
		fmt.Println("Error dialing Mongo host", err)
		return nil, err
	}
	db := s.DB(options.DBName)
	return db.C(options.Collection), nil
}

func run(options Options) error {
	//c, err := newConn(options)
	//if err != nil {
	//	return err
	//}
	//err = c.EnsureIndex(mgo.Index{Key: []string{"key"}})
	//if err != nil {
	//	fmt.Println("Error creating index", err)
	//	return err
	//}
	var counter int64
	var insertCounter int64 = 1
	var wg sync.WaitGroup
	for i := 0; i < int(options.Concurrency); i++ {
		wg.Add(1)
		fmt.Println("spawning worker")
		go worker(&counter, &insertCounter, &wg, options)
	}
	wg.Wait()
	fmt.Printf("Executed %d ops (%d inserts)\n", counter, insertCounter)
	return nil
}

func worker(counter *int64, insertCounter *int64, wg *sync.WaitGroup, options Options) {
	c, _ := newConn(options)
	for atomic.LoadInt64(counter) < options.Ops {
		st := time.Now()
		op := rand.Float64() < options.Mix
		var opName string
		if op {
			key := atomic.AddInt64(insertCounter, 1)
			c.Insert(genDoc(key))
			opName = "insert"
		} else {
			key := rand.Intn(int(atomic.LoadInt64(insertCounter)))
			res := bson.M{}
			c.Find(bson.M{"key": key}).One(&res)
			opName = "find"
		}
		v := atomic.AddInt64(counter, 1)
		if !options.Silent {
			fmt.Println(v, time.Now().Sub(st).Nanoseconds(), opName)
		}
	}
	wg.Done()
}
