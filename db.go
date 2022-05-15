package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// This is an append-only file. Note that the benefit of this is more useful when including deletion records and compaction, although
// this toy implementation does not include those features. It is append only as writing a new line into the file is an extremely
// efficient operation.
const dbName = "log-structure.db"

// Our hash index which is stored on disk, alongside our database. This mimics the functionality of being resilient to a crash, if we were
// to store our index entirely in-memory, then we would lose our entire hash table when a crash occurs. Instead, we can read it from disk
// on startup, if there is one present, and then hold it in memory for extremely fast read access to the database.
const indexName = "hash-index.db"

var (
	entry        = flag.String("entry", "", "a string entry to insert, should be in the form '<id>,<string>'")
	getId        = flag.String("get", "", "the ID of the entry to retrieve from the database.")
	disableIndex = flag.Bool("disable-index", false, "disable the hash index for retrieving an entry, forcing a search through the entire database.")

	// Our hash index is in the format { ID : byte_offset }
	// This enables us to jump to the relevant section of the file if the ID we are looking for
	// is contained within the hash index.
	hashIndex = make(map[string]int64)
)

func init() {
	flag.Parse()
}

// Get retrieves the entry with the given id from the file. This is intended to imitate the functionality of
// db_get() {
//     grep "^$1," database | sed -e "s/^$1,//" | tail -n 1
// }
// which is demonstrated in the book.
func Get(db *os.File, id string) (string, error) {

	r := bufio.NewScanner(db)
	var entry string

	// Jump straight into a full scan if the cache is disabled.
	if *disableIndex {
		fmt.Println("Indexing disabled, running full scan.")
		return scanFullDB(r, id), nil
	}

	if offset, ok := hashIndex[id]; ok {

		// Seek to our byte offset provided by the hash index, this means we only scan the entry from here
		// as opposed to the entire file.
		_, err := db.Seek(offset, io.SeekStart)
		if err != nil {
			return "", err
		}

		// Move to the next token, by default this is our new line ("\n") delimiter which is what we want,
		// this will be our record.
		r.Scan()

		// Return the text found at the byte offset, this is our desired entry.
		entry = r.Text()
		return entry, nil
	}

	// If the ID is not in our index, we need to scan the all the entries and then pass the latest one.
	// We cannot pass the first one, since there may be more up to date record in the file.
	// For practically all cases, the index will be present since we hold it in memory and update it
	// on each write. Although for full functionality, this is included to show that we would require a
	// full scan to find the latest entry.
	return scanFullDB(r, id), nil

}

func scanFullDB(sc *bufio.Scanner, id string) string {
	var entry string
	for sc.Scan() {

		// Values are in format of "<id>,<string>"
		dbId := strings.Split(sc.Text(), ",")[0]

		// Find all entries which match the ID, there may be multiple
		// so we find them all and only want the latest entry, which is what we return.
		// Note: This toy implementation does not include tombstone records for deletions.
		if dbId == id {
			entry = sc.Text()
		}
	}

	if entry == "" {
		return entry
	} else {
		// Return the most recent entry
		return entry
	}
}

// Set will append an entry into the given file. This attempts to imitate the functionality of
// db_set() {
//     echo "$1,$2" >> database
// }
// from the simplified database in the book.
func Set(db *os.File, hash *os.File, entry string) error {

	info, err := db.Stat()
	if err != nil {
		return err
	}

	// The actual implementation would likely write the data as binary, but to show the concept here we can
	// use string so that we can see the entires plainly in the database file.
	_, err = db.WriteString(entry + "\n")
	if err != nil {
		return err
	}

	// With the format of our entries, the ID is the 0th element using the comma seperator.
	id := strings.Split(entry, ",")[0]

	// Maintain hash index on writes, this is where a hash index trade-off occurs.
	// We need to maintain the offsets on writes, but it vastly speeds up reads.
	// This likely isn't a fully realistic imitation, since we're not doing any
	// compaction or segmenting of files, but the general concept is there.
	hashIndex[id] = info.Size()

	// Seek to the beginning of the file, we can overwrite our map, rather than appending to make it simpler.
	// We only maintain a single mapping value, rather than multiple and being required to read the latest entry.
	hash.Seek(0, io.SeekStart)

	// Update our hash index on subsequent data entries
	g := json.NewEncoder(hash)
	err = g.Encode(hashIndex)
	if err != nil {
		return err
	}

	return nil
}

func main() {

	f, err := os.OpenFile(dbName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	hashFile, err := os.OpenFile(indexName, os.O_RDWR|os.O_CREATE, 0666)
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
		err := Set(f, hashFile, *entry)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	// Get an entry using its ID. We're assuming that the ID is a known quantity here.
	if *getId != "" {
		fmt.Printf("Getting record with ID: %s\n", *getId)

		entry, err := Get(f, *getId)
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
