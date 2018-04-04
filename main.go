package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"dynamo.cli/db"
	"dynamo.cli/executors"
	"dynamo.cli/sqlparser"
	"dynamo.cli/tables"
	"github.com/briandowns/spinner"
	prompt "github.com/c-bata/go-prompt"
	"golang.org/x/net/context"
	cli "gopkg.in/urfave/cli.v2"
)

// TODO better suggest, suggest based on hash key and range key
var tableNameSuggestions []prompt.Suggest

// Key bindings, reserved, might use them oneday
var keyBindings = []prompt.KeyBind{
	{
		Key: prompt.ControlK,
		Fn:  func(buf *prompt.Buffer) {},
	},
}

func sqlRunner(sql string, resultCh chan string, errCh chan error) (chan string, chan error) {
	if sqlparser.SelectRegexp.MatchString(sql) {
		// add surfix "END" for rexexp matching
		if r, err := executors.Select(sql + " END"); err == nil {
			resultCh <- r
		} else {
			errCh <- err
		}
	} else if sqlparser.DescRegexp.MatchString(sql) && sqlparser.TableRegexp.MatchString(sql) {
		if r, err := executors.DescribeTable(sql + " END"); err == nil {
			resultCh <- r
		} else {
			errCh <- err
		}
	} else if sqlparser.UpdateRegexp.MatchString(sql) {
		// TODO require WHERE field, update all seems not so safe?
		if r, err := executors.Update(sql + " END"); err == nil {
			resultCh <- r
		} else {
			errCh <- err
		}
	} else {
		resultCh <- ""
		errCh <- nil
	}
	// TODO support SHOW TABLE
	return resultCh, errCh
}

// executor executes command and print the output.
func executor(in string) {
	s := strings.TrimSpace(in)
	s = strings.TrimSuffix(in, ";")
	if s == "" {
		return
	} else if s == "quit" || s == "exit" {
		os.Exit(0)
	} else {
		spin := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
		spin.Start()
		defer spin.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt) // sigCh only listens to os.Interrupt
		// Listen to the os interrupt signal which is ctrl+c
		// when ctrl+c is pressed, cancel current query
		go func() {
			select {
			case <-sigCh:
				cancel()
				return
			}
		}()

		resultCh := make(chan string, 1)
		errCh := make(chan error, 1)
		go sqlRunner(s, resultCh, errCh)

		// The main executor function will have to wait until the query is done or canceled
		// so that new prompts won't popup
		select {
		case <-ctx.Done():
			return
		case r := <-resultCh:
			fmt.Println(r)
		case e := <-errCh:
			fmt.Println(e)
		}
	}
}

func runPrompt(tablePrefix string) {
	spin := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	spin.Start()
	// load some table names for auto complete
	tableNames, _ := db.ListTable([]*string{}, nil)
	for _, name := range tableNames {
		// filter certain table name
		if tablePrefix != "" {
			if strings.HasPrefix(*name, tablePrefix) {
				tableNameSuggestions = append(tableNameSuggestions, prompt.Suggest{
					Text:        *name,
					Description: "table",
				})
				go func(tableName *string) {
					if _, err := tables.GetTableDesc(tableName); err != nil {
						fmt.Println(err)
					}
				}(name)
			}
		} else {
			tableNameSuggestions = append(tableNameSuggestions, prompt.Suggest{
				Text:        *name,
				Description: "table",
			})
			go func(tableName *string) {
				if _, err := tables.GetTableDesc(tableName); err != nil {
					fmt.Println(err)
				}
			}(name)
		}
	}
	spin.Stop()
	p := prompt.New(
		executor,
		completer,
		prompt.OptionPrefix(">>> "),
		prompt.OptionTitle("DynamoDB prompt"),
		prompt.OptionAddKeyBind(keyBindings...),
	)
	p.Run()
}

// completer returns the completion items from user input.
func completer(d prompt.Document) []prompt.Suggest {
	keywords := []prompt.Suggest{
		{Text: "SELECT", Description: "keyword"},
		{Text: "FROM", Description: "keyword"},
		{Text: "WHERE", Description: "keyword"},
		{Text: "LIMIT", Description: "keyword"},
		{Text: "DESC", Description: "keyword"},
		{Text: "TABLE", Description: "keyword"},
		{Text: "LIKE", Description: "keyword"},
		{Text: "ALL", Description: "keyword"},
		{Text: "AND", Description: "keyword"},
		{Text: "UPDATE", Description: "keyword"},
		{Text: "SET", Description: "keyword"},
		{Text: "RETRUNING", Description: "keyword"},
	}

	wordBefore := d.GetWordBeforeCursor()
	if wordBefore == "" {
		return []prompt.Suggest{}
	}
	if d.TextBeforeCursor() == " " {
		return tableNameSuggestions
	}
	if wordBefore == strings.ToUpper(wordBefore) && wordBefore != "_" && wordBefore != "-" && wordBefore != " " {
		return prompt.FilterHasPrefix(keywords, d.GetWordBeforeCursor(), true)
	}
	return prompt.FilterHasPrefix(tableNameSuggestions, d.GetWordBeforeCursor(), true)
}

func main() {
	// grmon.Start()
	defer recover()
	var accessKeyID string
	var secretAccessKey string
	var region string
	var tablePrefix string
	app := &cli.App{
		Name:    "dynamo.cli",
		Usage:   "DynamoDB command line prompt",
		Version: "0.1.0",
		Authors: []*cli.Author{&cli.Author{
			Name:  "xinbg",
			Email: "xbgxwh@outlook.com",
		}},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "key",
				Usage:       "specify aws config access key id",
				Aliases:     []string{"k"},
				Destination: &accessKeyID,
			},
			&cli.StringFlag{
				Name:        "secret",
				Usage:       "specify aws config secret access key",
				Aliases:     []string{"s"},
				Destination: &secretAccessKey,
			},
			&cli.StringFlag{
				Name:        "region",
				Usage:       "specify aws config region",
				Aliases:     []string{"r"},
				Destination: &region,
			},
			&cli.StringFlag{
				Name:        "tablePrefix",
				Usage:       "specify certain prefix string for table names auto completion",
				Aliases:     []string{"p"},
				Destination: &tablePrefix,
			},
		},
		Action: func(c *cli.Context) error {
			if region == "" {
				fmt.Println("Must provide aws config region")
			} else if accessKeyID == "" {
				fmt.Println("Must provide aws config access key id")
			} else if secretAccessKey == "" {
				fmt.Println("Must provide aws config secret access key")
			} else {
				db.GetDynamoSession(accessKeyID, secretAccessKey, region)
				runPrompt(tablePrefix)
			}
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println(err.Error())
	}
}
