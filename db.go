package logstructured

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

type DB struct {
	DB           *os.File         // Database file written to disk
	Hash         map[string]int64 // Hash index for fast lookups to the byte offset of the string value.
	HashDisabled bool             // Force a full scan, no use of the Hash index
	HashStorage  *os.File         // Hash index file, this is written to disk for persistence and durability between crashes etc. It can simply be loaded again on startup.
	sync.Mutex                    // Simple lock for writes to ensure safer concurrency.
}

// Get retrieves the entry with the given id from the file. This is intended to imitate the functionality of
// db_get() {
//     grep "^$1," database | sed -e "s/^$1,//" | tail -n 1
// }
// which is demonstrated in the book.
func Get(db *DB, id string) (string, error) {

	// No locks are required here since we are reading from a file, this is safe.

	r := bufio.NewScanner(db.DB)
	var entry string

	// Jump straight into a full scan if the cache is disabled.
	if db.HashDisabled {
		fmt.Println("Indexing disabled, running full scan.")
		return scanFullDB(r, id), nil
	}

	if offset, ok := db.Hash[id]; ok {

		// Seek to our byte offset provided by the hash index, this means we only scan the entry from here
		// as opposed to the entire file.
		_, err := db.DB.Seek(offset, io.SeekStart)
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
func Set(db *DB, entry string) error {

	info, err := db.DB.Stat()
	if err != nil {
		return err
	}

	// The actual implementation would likely write the data as binary, but to show the concept here we can
	// use string so that we can see the entires plainly in the database file.
	_, err = db.DB.WriteString(entry + "\n")
	if err != nil {
		return err
	}

	// With the format of our entries, the ID is the 0th element using the comma seperator.
	id := strings.Split(entry, ",")[0]

	// Maintain hash index on writes, this is where a hash index trade-off occurs.
	// We need to maintain the offsets on writes, but it vastly speeds up reads.
	// This likely isn't a fully realistic imitation, since we're not doing any
	// compaction or segmenting of files, but the general concept is there.
	db.Hash[id] = info.Size()

	// Seek to the beginning of the file, we can overwrite our map, rather than appending to make it simpler.
	// We only maintain a single mapping value, rather than multiple and being required to read the latest entry.
	_, err = db.HashStorage.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Update our hash index on subsequent data entries
	g := json.NewEncoder(db.HashStorage)
	err = g.Encode(db.Hash)
	if err != nil {
		return err
	}

	return nil
}
