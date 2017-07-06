/*Package gzlines - Sometimes you just want to iterate lines from GZ files.

This is a job that's made surprisingly awkward in stock Go, so I made some
helper functions.

Background: I like to store my data in JSON-Lines format, GZip-compressed. So
a typical workflow to use a dataset is to load every file in a directory,
gzip-decompress, and iterate them line-wise. In Python, this is trivial. In go,
it's a bit less fun.

This is awkward but trivial code. Consider it public domain.
If you need something more explicit, I hereby license it under the Creative Commons
Zero license in all jurisdictions in which I hold a right to copyright.
*/
package gzlines

import (
	"bufio"
	"compress/gzip"
	"os"
	"path/filepath"
	"sync"
)

var (
	jlgzGlobs = []string{
		"*.gz",
		"*.gzip",
	}

	// LineBufferLengthFactor is how much room to allocate for the line-wise buffer.
	// If your files have very long lines, increase this.
	LineBufferLengthFactor = 1024
)

// AllGZInDir is a helper to return the filenames of all JL-Gz files in a directory.
// It uses, for globbing, the following patterns: *.jl.gz, *.jlgz, *.jsonl.gz, *.jsonlgz
func AllGZInDir(dir string) []string {
	var (
		dFg   string
		fs    []string
		allFs []string
		err   error
	)
	for _, fg := range jlgzGlobs {
		dFg = filepath.Join(dir, fg)
		if fs, err = filepath.Glob(dFg); err != nil {
			panic(err)
		}
		allFs = append(allFs, fs...)
	}
	return allFs
}

// LinesOfGZ sends successive lines of a Gzip-encoded file.
func LinesOfGZ(rawf *os.File) (chan []byte, chan error, error) {
	rawContents, err := gzip.NewReader(rawf)
	if err != nil {
		return nil, nil, err
	}
	contents := bufio.NewScanner(rawContents)
	cbuffer := make([]byte, 0, bufio.MaxScanTokenSize)
	contents.Buffer(cbuffer, bufio.MaxScanTokenSize*LineBufferLengthFactor) // Otherwise long lines crash the scanner.
	ch := make(chan []byte)
	errs := make(chan error)
	go func(ch chan []byte, errs chan error, contents *bufio.Scanner) {
		defer func(ch chan []byte, errs chan error) {
			close(ch)
			close(errs)
		}(ch, errs)
		var (
			b   []byte
			err error
		)
		for contents.Scan() {
			b = append([]byte{}, contents.Bytes()...)
			ch <- b
		}
		if err = contents.Err(); err != nil {
			errs <- err
			return
		}
	}(ch, errs, contents)
	return ch, errs, nil
}

// Used from MultiplexGZLines to collect lines and errors.
func sendFromFile(wg *sync.WaitGroup, rawf *os.File, combinedlc chan []byte, combinederr chan error) {
	var (
		err error
	)
	defer wg.Done()
	lc, ec, err := LinesOfGZ(rawf)
	if err != nil {
		combinederr <- err
		return
	}
	for {
		select {
		case l := <-lc:
			{
				if l == nil {
					return
				} // Chan closed
				combinedlc <- l
			}
		case e := <-ec:
			{
				if e == nil {
					return
				} // Chan closed
				combinederr <- e
			}
		}
	}
}

// MultiplexGZLines is a high-level function that takes a list of filenames for
// Gzip-compressed JSON-Lines files, and returns lines from this pool of files
// without guaranteed order. It's assumed that you JSON-decode the lines or
// otherwise query them as they are streamed to the calling function.
func MultiplexGZLines(filenames ...string) (chan []byte, chan error, error) {
	combinedlc := make(chan []byte)
	combinederr := make(chan error)
	wg := new(sync.WaitGroup)
	for _, fn := range filenames {
		wg.Add(1)
		rawf, err := os.Open(fn)
		if err != nil {
			return combinedlc, combinederr, err
		}
		go sendFromFile(wg, rawf, combinedlc, combinederr)
	}
	// Await all the above, then close the channels so they can be iterated cleanly.
	go func(wg *sync.WaitGroup, clc chan []byte, cec chan error) {
		wg.Wait()
		close(clc)
		close(cec)
	}(wg, combinedlc, combinederr)
	return combinedlc, combinederr, nil
}
