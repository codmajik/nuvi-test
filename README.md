# nuvi-test

## how to run
- go install github.com/codmajik/nuvi-test
- $GOPATH/bin/nuvi-test


### supported paramters
  -redis_url string: url to redis in the form redis://HOSTNAME_OR_IP:PORT  
  -url string: url to directory index of zip files http[s]://URL  
  -limit int: process a specified number of files from the fetched list
  
  
### running test
go test -v
