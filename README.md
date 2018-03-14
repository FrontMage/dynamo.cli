[![Go Report Card](https://goreportcard.com/badge/github.com/FrontMage/dynamo.cli)](https://goreportcard.com/report/github.com/FrontMage/dynamo.cli)

### A sql like command line prompt for AWS DynamoDB

---

Here is the situation, someone, likely your pm, tells you to find some user with a name like James Bond.

You fogot pin that aws console to chrome, and you just can't remember how to use that crapy aws cli.

---

So skip the crap, just use `dynamo.cli`, the only thing you ever need to know is `SQL`.

![Screenshot](screenshots/screenshot.gif)

```
NAME:
   dynamo.cli - DynamoDB command line prompt

USAGE:
   dynamo.cli [global options] command [command options] [arguments...]

VERSION:
   0.0.0

COMMANDS:
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --key value, -k value          specify aws config access key id
   --secret value, -s value       specify aws config secret access key
   --region value, -r value       specify aws config region
   --tablePrefix value, -p value  specify certain prefix string for table names auto completion
   --help, -h                     show help (default: false)
   --version, -v                  print the version (default: false)

```

### Install

##### Install from source

Note this method requires [dep](https://github.com/golang/dep) command installed.

`git clone git@github.com:FrontMage/dynamo.cli.git $GOPATH/src/dynamo.cli`

`cd $GOPATH/src/dynamo.cli`

`dep ensure`

`go install`

And you need [awscli](https://aws.amazon.com/cli/) installed and configured, even after I passed this keys to the `aws-go-sdk`, you still need that...

If any one knows a way to skip this, please do share.

##### Install by download release binary

Checkout the latest released binary [here](https://github.com/FrontMage/dynamo.cli/releases) .

Please do make an alias, otherwise you need specify your credentials everytime you do this.

`alias dmcli="dynamo.cli -k yourKeyId -s yourSecretKey -r yourRegion"`

---

`SELECT userId,name FROM user WHERE name=9527 LIMIT 10`

Currently only supports `SELECT` and `UPDATE`, now tring to support `DELETE` and `JOIN`.

`Ctrl + c` can't terminate running query because I haven't figure out how to do this.
```
After some digging, there is a context package for golang,
howerver it can't quit a running function unless the function 
is checking context.Done()on every step of its body.

This can be used to just ignore current running query, 
but the query will continue in the background.

I suppose it's not possible to cancel the request after the request is sent,
so maybe consider a rollback action?

Version 0.0.4 supports ctrl+c to ignore running query.
```

Which will be handy when that pm tells you to change somebody's coins to 2^10.

---

##### Road Map

Mostly all that `TODO` in the code, and tell others about this project.
