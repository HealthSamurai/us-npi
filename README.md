# USNPI

[npi]: http://download.cms.gov/nppes/NPI_Files.html
[fhir]:https://www.hl7.org/fhir/
[bundle]:https://www.hl7.org/fhir/bundle.html

[US NPI][npi] registry in [FHIR][fhir].

## REST endpoints

### Practitioner

- `GET /practitioner/<ID>`

  Returns a single practitioner by its ID or `404 Not Found` when not found or
  marked as deleted. Example:

  ```bash
  curl https://npi.health-samurai.io/practitioner/1538375811
  ```

- `GET /practitioner/$batch?ids=<id1,id2,...,idn>`

  Returns multiple practitioners at once by their IDs. The `ids` required
  parameter is a string of NPI ids separated by commas. Returns `400 Bad
  Request` response when not passed or malformed. The positive response is a
  [Bundle node][bundle] contains child nodes. Example:

  ```bash
  https://npi.health-samurai.io/practitioner/$batch?ids=1538375811,1447466727
  ```

- `GET /practitioner?q=<term>&_count=<count>`

  Returns either a list of random practitioners or, if a query term was passed,
  a search result. The `q` is an optional query term that searches across
  multiple fields including the last, middle and first names, city and
  state. The `_count` is an optional integer to limit the number or entries in
  the result. Returns a [Bundle node][bundle]. Example:

  ```bash
  https://npi.health-samurai.io/practitioner?q=david
  ```

### Organizations

### System status

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
make uberjar-build uberjar-run
```

or just

```
make
```

## TODO

* Taxonomy terminology
* Implement _elements to get only some elements form resource_

## License

Copyright © 2017 niquola

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
