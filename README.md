# usnpi

US NPI registy in FHIR

## Install

See deploy folder and ci3.yaml

## Local development

Crate a new Git-ignored `env_dev` file:

```bash
touch env_dev
```

Provide your own Postgres credentials, paths, etc:

```bash
$ cat env_dev
export DATABASE_URL="jdbc:postgresql://localhost:5432/usnpi?stringtype=unspecified&user=<user>&password=<password>"
export FHIRTERM_BASE=/path/to/FHIRTERM_BASE
export BEAT_TIMEOUT=30
```

Load the config:

```bash
source env_dev
```

Then run either REPL or the compiled uberjar:

```bash
lein repl
# or
java -jar ./target/usnpi.jar
```

Make sure you've got `sed`, `unzip` and `7z` command line tools installed and
accessible from your PATH.

## TODO

* Make it autonomous - i.e. save loaded months; check for updates - download and install udpates
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
