package main

import (
	"archive/zip"
	"bufio"
	"flag"
	//"fmt"
	"github.com/garyburd/redigo/redis"
	"golang.org/x/net/html"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"sync"
	//	"time"
)

const REDIS_LIST_NAME = "NEWS_XML"

var (
	//for testing only -- can't download 9GB per test cycle
	limit     = flag.Int("limit", -1, "testing only: do a limited round")
	URL       = flag.String("url", "http://feed.omgili.com/5Rh5AMTrc4Pv/mainstream/posts/", "url to use")
	redisHost = flag.String("redis_url", "redis://:6379", "redis://HOST:PORT")

	processed = make(map[string]bool)
)

type IRange interface {
	Next() (string, bool)
	Reader(string) (*zip.Reader, error)
	CleanUp(string)
}

type ChannelRange struct {
	zips chan string
	re   *zip.ReadCloser
}

func (c *ChannelRange) Next() (string, bool) {
	if c.re != nil {
		c.re.Close()
		c.re = nil
	}

	fileName, ok := <-c.zips
	return fileName, ok
}

func (c *ChannelRange) Reader(fileName string) (*zip.Reader, error) {
	re, err := zip.OpenReader(fileName)

	if err != nil {
		return nil, err
	}

	c.re = re
	return &re.Reader, nil
}

func (c *ChannelRange) CleanUp(fileName string) {
	if c.re != nil {
		c.re.Close()
		c.re = nil
	}

	os.Remove(fileName)
}

func extractNstore(conn redis.Conn, r IRange, wg *sync.WaitGroup) {

	wg.Add(1)
	defer wg.Done()

	err := conn.Send("MULTI")
	if err != nil {
		log.Panicln(err)
	}

	//remove this key in this transaction
	conn.Send("DEL", REDIS_LIST_NAME)

	defer func() {
		_, err := conn.Do("EXEC")
		if err != nil {
			log.Println("unable to store data in redis", err)
			return
		}
	}()

	for {

		fileName, ok := r.Next()
		if !ok {
			break
		}

		log.Println("unzipping and storing", fileName)

		f, err := r.Reader(fileName)
		if err != nil {
			log.Println("error: opening zip file", fileName)
			continue
		}

		for _, z := range f.File {

			log.Println("opening: item from ", fileName, "=>", z.Name)

			r, err := z.Open()
			if err != nil {
				log.Println("error:", z.Name, "failed", err)
				continue
			}

			content, err := ioutil.ReadAll(r)
			if err == nil {
				err := conn.Send("LPUSH", REDIS_LIST_NAME, string(content))
				if err != nil {
					panic(err) // this should not happen
				}
			}

			err = r.Close()
			if err != nil {

			}
		}

		r.CleanUp(fileName)
		log.Println("done:", fileName)
	}
}

func downloadToFile(baseUri *url.URL, fileName string) (string, error) {

	fullpath := filepath.Join(os.TempDir(), "nuvi-test"+fileName)

	endpointUrl, err := url.Parse(fileName)
	if err != nil {
		log.Println("error: Parse file link", fileName, ". failed =>", err)
		return "", err
	}

	fullUrl := baseUri.ResolveReference(endpointUrl)
	log.Println("downloading", fullUrl.String(), "to", fullpath)

	r, err := http.Get(fullUrl.String())
	if err != nil {
		log.Printf("downloading %s failed with %s\n", fullUrl.String(), err)
		return "", err
	}

	defer r.Body.Close()

	//NOTE:
	// Implement some resumption and local file caching scheme
	// but that is out of scope for this test

	F, err := os.Create(fullpath)
	if err != nil {
		return "", err
	}

	defer F.Close()

	_, err = bufio.NewReader(r.Body).WriteTo(F)
	return fullpath, err
}

func extractFileList(reader io.Reader) []string {
	is_zipfile, _ := regexp.Compile("^.+\\.zip$")
	links := make([]string, 0, 100)

	indexPage := html.NewTokenizer(reader)
	for {
		tokenType := indexPage.Next()
		if tokenType == html.ErrorToken {
			break
		}

		token := indexPage.Token()
		if tokenType == html.StartTagToken && token.DataAtom.String() == "a" {

			for _, attr := range token.Attr {
				if attr.Key != "href" {
					continue
				}

				if is_zipfile.MatchString(attr.Val) {
					links = append(links, attr.Val)
				}

				break
			}
		}
	}
	return links
}

func extractLinks(uri string) ([]string, error) {
	log.Println("fetching directory index ", uri)

	resp, err := http.Get(uri)
	if err != nil {
		log.Printf("error: fetching dir index from %s: %s\n", uri, err)
		return nil, err
	}

	defer resp.Body.Close()

	links := extractFileList(resp.Body)

	log.Println(len(links), "files found in directory")
	return links, nil
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU()) // use all available CPU

	conn, err := redis.DialURL(*redisHost)
	if err != nil {
		log.Println("Connection to Redis Failed", err)
		return
	}

	defer conn.Close()

	baseUri, err := url.Parse(*URL)
	if err != nil {
		log.Println("error: Invalid url", *URL, err)
		return
	}

	links, err := extractLinks(baseUri.String())
	if err != nil {
		log.Printf("error: fetching dir index from %s: %s\n", baseUri.String(), err)
		return
	}

	sort.Strings(links)

	if *limit > -1 {
		log.Println("processing", *limit, "of", len(links))
		links = links[:(*limit)]

	}

	var wg, wg_extractor sync.WaitGroup

	zipChan := make(chan string)
	go extractNstore(conn, &ChannelRange{zipChan, nil}, &wg_extractor)

	link_ch := make(chan string)
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()

			for {
				uri, ok := <-link_ch
				if !ok {
					return
				}

				fullpath, err := downloadToFile(baseUri, uri)
				if err != nil {
					if fullpath != "" {
						os.Remove(fullpath)
					}
					continue
				}

				//send this file to be processed
				zipChan <- fullpath
			}

		}()
	}

	for _, val := range links {

		if _, ok := processed[val]; ok {
			continue
		}

		processed[val] = false
		link_ch <- val
	}

	close(link_ch)
	wg.Wait()

	close(zipChan)
	wg_extractor.Wait()
}
