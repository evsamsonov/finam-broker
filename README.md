# finam-broker

An implementation of [trengin.Broker](http://github.com/evsamsonov/trengin) using [Finam Trade API](https://finamweb.github.io/trade-api-docs/) 
for creating automated trading robots. 

## Features
- Opens position, changes stop loss and take profit, closes position.
- Tracks open position.
- Supports multiple open positions at the same time.
- Commission in position is approximate.

## How to use

Create a new `Finam` object using constructor `New`. Pass [full-access token](https://finamweb.github.io/trade-api-docs/tokens),
client id. 

```go
package main

import (
	"context"
	"log"

	"github.com/evsamsonov/trengin/v2"
	"github.com/evsamsonov/finam-broker"
)

func main() {
	finamBroker := fnmbroker.New(
		"token",
		"123",
		// options...
	)

	tradingEngine := trengin.New(&Strategy{}, finamBroker)
	if err := tradingEngine.Run(context.Background()); err != nil {
		log.Fatal("Trading engine crashed")
	}
}

type Strategy struct{}
func (s *Strategy) Run(ctx context.Context, actions trengin.Actions) error { panic("implement me") }
```

See more details in [trengin documentation](http://github.com/evsamsonov/trengin).

### Option

You can configure `Finam` to use `Option`

| Methods                       | Returns Option which                                                             |
|-------------------------------|----------------------------------------------------------------------------------|
| `WithLogger`                  | Sets logger. The default logger is no-op Logger.                                 |
| `WithProtectiveSpreadPercent` | Sets protective spread in percent for executing orders. The default value is 1%. |
| `WithUseCredit`               | Sets using credit funds for executing orders.                                    |
| `WithSecurityCacheFile`       | Sets path to securities cache file. Default is `./securities.json`               |

## Checkup

Use `finam-checkup` for checking the ability to trade with a specific token and client id. 

### How to install

```bash
go install github.com/evsamsonov/tinkoff-broker/cmd/finam-checkup@latest
```

### How to use 

```bash
finam-checkup [CLIENT_ID] [SECURITY_BOARD] [SECURITY_CODE] [-v]
 ```

| Flag | Description         |
|------|---------------------|
| `-v` | Print logger output |

## Development

### Makefile 

Makefile tasks are required docker and golang.

```bash
$ make help    
doc                            Run doc server using docker
lint                           Run golang lint using docker
pre-push                       Run golang lint and test
test                           Run tests
```
