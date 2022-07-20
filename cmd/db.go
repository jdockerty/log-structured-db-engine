package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	logstructured "github.com/jdockerty/log-structured-db-engine"
)

var (
	entry        = flag.String("entry", "", "a string entry to insert, should be in the form '<id>,<string>'")
	getId        = flag.String("get", "", "the ID of the entry to retrieve from the database.")
	disableIndex = flag.Bool("disable-index", false, "disable the hash index for retrieving an entry, forcing a search through the entire database.")

	// This is an append-only file. Note that the benefit of this is more useful when including deletion records and compaction, although
	// this toy implementation does not include those features. It is append only as writing a new line into the file is an extremely
	// efficient operation.
	dbName = flag.String("db-file", "log-structure.db", "Database file to use or create")

	// Our hash index which is stored on disk, alongside our database. This mimics the functionality of being resilient to a crash, if we were
	// to store our index entirely in-memory, then we would lose our entire hash table when a crash occurs. Instead, we can read it from disk
	// on startup, if there is one present, and then hold it in memory for extremely fast read access to the database.
	indexName = flag.String("index-file", "hash-index.db", "The hash index file to create or load from disk if it doesn't already exist")

	// Our hash index is in the format { ID : byte_offset }
	// This enables us to jump to the relevant section of the file if the ID we are looking for
	// is contained within the hash index.
	hashIndex = make(map[string]int64)
)

func main() {

	flag.Parse()

	f, err := os.OpenFile(*dbName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	hashFile, err := os.OpenFile(*indexName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = hashFile.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	info, err := hashFile.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// In our toy example, this will basically always be the case, but serves
	// as a general idea of how this might be implemented.
	if info.Size() > 0 {
		fmt.Println("Populating stored hash index")

		// Read our saved hash index from disk, this is our crash tolerance.
		d := json.NewDecoder(hashFile)
		err := d.Decode(&hashIndex)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Write an entry.
	if *entry != "" {
		if !strings.Contains(*entry, ",") {
			log.Fatal("an entry should be in the format '<id>,<string>', e.g. '10,hello'")
		}
		err := logstructured.Set(hashIndex, f, hashFile, *entry)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	// Get an entry using its ID. We're assuming that the ID is a known quantity here.
	if *getId != "" {
		fmt.Printf("Getting record with ID: %s\n", *getId)

		entry, err := logstructured.Get(hashIndex, disableIndex, f, *getId)
		if err != nil {
			log.Fatal(err)
		}

		if entry == "" {
			fmt.Printf("ID '%s' is not contained in the database.\n", *getId)
			return
		}
		fmt.Println("Record:", entry)
		return

	}
}
