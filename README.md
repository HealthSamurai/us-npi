# USNPI

[npi]: http://download.cms.gov/nppes/NPI_Files.html
[fhir]:https://www.hl7.org/fhir/
[bundle]:https://www.hl7.org/fhir/bundle.html
[pract]:https://www.hl7.org/fhir/practitioner.html
[org]:https://www.hl7.org/fhir/organization.html

[US NPI][npi] registry in [FHIR][fhir].

## REST endpoints

### Practitioner

- `GET /practitioner/<ID>`

  Returns a single [Practitioner][pract] by its ID or `404 Not Found` when not
  found or marked as deleted. Example:

  ```bash
  curl https://npi.health-samurai.io/practitioner/1538375811
  ```

- `GET /practitioner/$batch?ids=<id1,id2,...,idn>`

  Returns multiple practitioners at once by their IDs. The `ids` required
  parameter is a string of NPI ids separated by commas. Returns `400 Bad
  Request` response when not passed or malformed. The positive response is a
  [Bundle node][bundle] contains child nodes. Example:

  ```bash
  curl https://npi.health-samurai.io/practitioner/$batch?ids=1538375811,1447466727
  ```

- `GET /practitioner?q=<term>&_count=<count>`

  Returns either a list of random practitioners or, if a query term was passed,
  a search result. The `q` is an optional query term that searches across
  multiple fields including the last, middle and first names, city and
  state. The `_count` is an optional integer to limit the number or entries in
  the result (100 by default). Returns a [Bundle node][bundle]. Example:

  ```bash
  curl https://npi.health-samurai.io/practitioner?q=david&_count=5
  ```

  If a query term carries several words inside it, say `foo bar baz`, the result
  logic will concatenate them with `AND`s as the following pseudo-code does:
  `search(foo) AND search(bar) AND search(baz)`

  A query term might have a prefix with colon to guarantee more accurate
  results. For example:

  - `g:David` searches by a given name;
  - `m:Charles` by a middle name;
  - `p:MD` by a prefix;
  - `z:JR` by a suffix;
  - `f:Thomson` by a family name;
  - `s:TX` by a USA state;
  - `c:Rogersville` by a city name.

### Organizations

- `GET /organization/<ID>`

  Returns a single [Organization][org] by its ID or 404 when not found or marked
  as deleted. Example:

  ```bash
  curl https://npi.health-samurai.io/organization/1972660348
  ```

- `GET /organization/$batch&ids=<id1,id2,...,idn>`

  Returns multiple organizations at once by their ids. The `ids` required
  parameter is a string of NPI ids separated by comma. The result is a [Bundle
  node][bundle] with child entities. Example:

  ```bash
  curl https://npi.health-samurai.io/organization/$batch?ids=1770796096,1700387479
  ```

- `GET /organization?q=<term>&_count=<count>`

  When `q` is not passed, returns random organization nodes. When it is, returns
  a search result made across an organization's name or an address. The result
  is a [Bundle node][bundle]. The `_count` is an optional integer to limit the
  result. Example:

  ```bash
  curl https://npi.health-samurai.io/organization?q=WALMART
  ```

  The system considers multiple words in a query term like `/practitioner?q=...`
  does.

  A query term might have a prefix with colon to guarantee more accurate
  results. For example:

  - `n:Walmart` searches by a name;
  - `s:TX` by a USA state;
  - `c:Rogersville` by a city name.

### System status

- `GET /system/env`

  Returns some of ENV variables, e.g. a Git commit hash. Example:

  ```bash
  curl https://npi.health-samurai.io/system/env
  ```

- `GET /system/updates`

  Returns a list of NPI downloaded and processed files. These file will be
  ignored when trying to process them once again. Example:

  ```bash
  curl https://npi.health-samurai.io/system/updates
  ```

- `GET /system/tasks`

  Returns a list of tasks with their schedule info and status. Example:

  ```bash
  curl https://npi.health-samurai.io/system/tasks
  ```

- `GET /system/beat`

  Returns a boolean flag that indicates whether a background task processor (also
  known as `beat`) is working or not. Example:

  ```bash
  curl https://npi.health-samurai.io/system/beat
  ```

- `GET /system/db`

  Returns the database cache statistics: how many cache blocks are spent on
  different relations (tables, indexes, etc). Also, returns the current cache
  settings. Example:

  ```bash
  curl https://npi.health-samurai.io/system/db
  ```

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
