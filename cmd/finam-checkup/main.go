/*
Finam-checkup checks all methods of Finam Broker.
It opens position, changes conditional orders, closes position.
This can be useful for development, checking the ability
to trade with a specific client id, security board and security code.

How to install:

	go install github.com/evsamsonov/finam-broker/cmd/finam-checkup@latest

Usage:

	finam-checkup [CLIENT_ID] [SECURITY_BOARD] [SECURITY_CODE] [flags]

The flags are:

	-v
	    Print logger output
*/
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"

	"github.com/evsamsonov/trengin/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"

	fnmbroker "github.com/evsamsonov/finam-broker"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println(
			"This command checks all methods of Finam Broker.\n" +
				"It opens position, changes conditional orders, closes position.",
		)
		fmt.Println("\nUsage: finam-checkup [CLIENT_ID] [SECURITY_BOARD] [SECURITY_CODE] [-v]")
		return
	}
	clientID := os.Args[1]
	securityBoard := os.Args[2]
	securityCode := os.Args[3]

	verbose := flag.Bool("v", false, "")
	if err := flag.CommandLine.Parse(os.Args[4:]); err != nil {
		log.Fatalf("Failed to parse args: %s", err)
	}

	checkupParams := NewCheckupParams(clientID, securityBoard, securityCode)
	if err := checkupParams.AskUser(); err != nil {
		log.Fatalf("Failed to get checkup params: %s", err)
	}

	checkuper, err := NewCheckuper(*verbose)
	if err != nil {
		log.Fatalf("Failed to create checkuper: %s", err)
	}
	if err := checkuper.CheckUp(checkupParams); err != nil {
		log.Fatalf("Failed to check up: %s", err)
	}
	fmt.Println("Check up is successful! 🍺")
}

type CheckUpArgs struct {
	clientID         string
	securityBoard    string
	securityCode     string
	token            string
	stopLossOffset   float64
	takeProfitOffset float64
	positionType     trengin.PositionType
}

func NewCheckupParams(clientID, securityBoard, securityCode string) CheckUpArgs {
	return CheckUpArgs{
		clientID:      clientID,
		securityBoard: securityBoard,
		securityCode:  securityCode,
	}
}

func (c *CheckUpArgs) AskUser() error {
	fmt.Printf("Paste Finam token: ")
	tokenBytes, err := term.ReadPassword(syscall.Stdin)
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	fmt.Println()
	c.token = string(tokenBytes)

	var positionType string
	fmt.Print("Enter position direction [long, short]: ")
	if _, err = fmt.Scanln(&positionType); err != nil {
		return fmt.Errorf("read stop loss indent: %w", err)
	}
	if positionType != "long" && positionType != "short" {
		return fmt.Errorf("read position direction: %w", err)
	}
	c.positionType = trengin.Long
	if positionType == "short" {
		c.positionType = trengin.Short
	}

	var stopLossOffset, takeProfitOffset float64
	fmt.Print("Enter stop loss indent [0 - skip]: ")
	if _, err = fmt.Scanln(&stopLossOffset); err != nil {
		return fmt.Errorf("read stop loss indent: %w", err)
	}
	c.stopLossOffset = stopLossOffset

	fmt.Print("Enter take profit indent [0 - skip]: ")
	if _, err = fmt.Scanln(&takeProfitOffset); err != nil {
		return fmt.Errorf("read take profit indent: %w", err)
	}
	c.takeProfitOffset = takeProfitOffset
	return nil
}

type Checkuper struct {
	logger *zap.Logger
}

func NewCheckuper(verbose bool) (*Checkuper, error) {
	logger := zap.NewNop()
	if verbose {
		var err error
		logger, err = zap.NewDevelopment(zap.IncreaseLevel(zap.DebugLevel))
		if err != nil {
			return nil, fmt.Errorf("create logger: %w", err)
		}
	}
	return &Checkuper{
		logger: logger,
	}, nil
}

func (t *Checkuper) CheckUp(params CheckUpArgs) error {
	broker := fnmbroker.New(params.token, params.clientID, fnmbroker.WithLogger(t.logger))

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer cancel()
		if err := broker.Run(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("broker: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		defer cancel()
		t.WaitAnyKey("Press any key for open position...")

		openPositionAction := trengin.OpenPositionAction{
			Type:             params.positionType,
			SecurityBoard:    params.securityBoard,
			SecurityCode:     params.securityCode,
			Quantity:         1,
			StopLossOffset:   params.stopLossOffset,
			TakeProfitOffset: params.takeProfitOffset,
		}
		position, positionClosed, err := broker.OpenPosition(ctx, openPositionAction)
		if err != nil {
			return fmt.Errorf("open position: %w", err)
		}
		fmt.Printf(
			"Position was opened. Open price: %f, stop loss: %f, take profit: %f, commission: %f\n",
			position.OpenPrice,
			position.StopLoss,
			position.TakeProfit,
			position.Commission,
		)

		g.Go(func() error {
			select {
			case <-ctx.Done():
				return nil
			case pos := <-positionClosed:
				fmt.Printf(
					"Position was closed. Conditional orders was removed. "+
						"Close price: %f, profit: %f, commission: %f\n",
					pos.ClosePrice,
					pos.Profit(),
					position.Commission,
				)
			}
			return nil
		})
		t.WaitAnyKey("Press any key for reduce by half conditional orders...")

		changeConditionalOrderAction := trengin.ChangeConditionalOrderAction{
			PositionID: position.ID,
			StopLoss:   position.OpenPrice - params.stopLossOffset/2*position.Type.Multiplier(),
			TakeProfit: position.OpenPrice + params.takeProfitOffset/2*position.Type.Multiplier(),
		}
		position, err = broker.ChangeConditionalOrder(ctx, changeConditionalOrderAction)
		if err != nil {
			return fmt.Errorf("change condition order: %w", err)
		}
		fmt.Printf(
			"Conditional orders was changed. New stop loss: %f, new take profit: %f\n",
			position.StopLoss,
			position.TakeProfit,
		)
		t.WaitAnyKey("Press any key for close position...")

		closePositionAction := trengin.ClosePositionAction{PositionID: position.ID}
		position, err = broker.ClosePosition(ctx, closePositionAction)
		if err != nil {
			return fmt.Errorf("close position: %w", err)
		}
		return nil
	})

	return g.Wait()
}
func (t *Checkuper) WaitAnyKey(msg string) {
	fmt.Print(msg)
	_, _ = bufio.NewReader(os.Stdin).ReadBytes('\n')
}
