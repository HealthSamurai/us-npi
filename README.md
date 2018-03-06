# usnpi

US NPI registry in FHIR

## Install

See deploy folder and ci3.yaml

## Local development

Clone an `env` file to a Git-ignored `env_dev` as follows:

```bash
cp env env_dev
```

Edit this file and provide your own `DB_*` values. Load the config:

```bash
source env_dev
```

To run REPL:

```bash
make repl
```

To build and run an uberjar:

```
make
# or make uberjar-build uberjar-run
```

Make sure you've got `sed`, `unzip` and `7z` command line tools installed and
accessible from your PATH. You'll also need `psql` tool installed which is a
part of `postgresql-client` package.

## TODO

* Make it autonomous - i.e. save loaded months; check for updates - download and install updates
* Organization search
* Practitioner search
* Taxonomy terminology
* Implement prefix search with trigram
* Implement _elements to get only some elements form resource_
* Rewrite into jsonb

## License

Copyright © 2017 niquola

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
