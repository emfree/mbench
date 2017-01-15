package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

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
}

func main() {
	options := parseFlags()
	run(options)
}

func parseFlags() Options {
	var options Options
	flagParser := flag.NewParser(&options, flag.Default)
	flagParser.Parse()
	return options
}

func genDoc(key int64) map[string]interface{} {
	doc := map[string]interface{}{
		"key":  key,
		"name": "Bananarama",
		"data": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	return doc
}

func run(options Options) error {
	s, err := mgo.Dial(options.Host)
	if err != nil {
		return err
	}
	db := s.DB(options.DBName)
	c := db.C(options.Collection)
	var counter int64
	var insertCounter int64 = 1
	var wg sync.WaitGroup
	for i := 0; i < int(options.Concurrency); i++ {
		wg.Add(1)
		fmt.Println("spawning worker")
		go worker(c, &counter, &insertCounter, &wg, options)
	}
	wg.Wait()
	return nil
}

func worker(c *mgo.Collection, counter *int64, insertCounter *int64, wg *sync.WaitGroup, options Options) {
	for atomic.LoadInt64(counter) < options.Ops {
		op := rand.Float64() < options.Mix
		if op {
			key := atomic.AddInt64(insertCounter, 1)
			c.Insert(genDoc(key))
		} else {
			key := rand.Intn(int(atomic.LoadInt64(insertCounter)))
			c.Find(bson.M{"key": key})

		}
		v := atomic.AddInt64(counter, 1)
		fmt.Println(v)
	}
	wg.Done()
}
