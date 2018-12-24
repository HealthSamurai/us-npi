# US NPI

[npi]: http://download.cms.gov/nppes/NPI_Files.html
[fhir]:https://www.hl7.org/fhir/
[bundle]:https://www.hl7.org/fhir/bundle.html
[pract]:https://www.hl7.org/fhir/practitioner.html
[org]:https://www.hl7.org/fhir/organization.html

[US NPI][npi] registry on [FHIR][fhir].

# Table of Contents

<!-- toc -->

- [REST endpoints](#rest-endpoints)
  * [Base URL](#base-url)
  * [Practitioner](#practitioner)
  * [Organizations](#organizations)
  * [Other FHIR endpoints](#other-fhir-endpoints)
  * [System status](#system-status)
- [Install](#install)
- [Local development](#local-development)
- [TODO](#todo)
- [License](#license)

<!-- tocstop -->

## REST endpoints

### Base URL

- `https://npi.aidbox.app`

### Practitioner

- `GET /practitioner/<ID>`

  Returns a single [Practitioner][pract] by their ID, or `404 Not Found` when not
  found or marked as deleted. Example:

  ```bash
  curl https://npi.aidbox.app/practitioner/1538375811
  ```

- `GET /practitioner/$batch?ids=<id1,id2,...,idn>`

  Returns multiple practitioners at once by their IDs. The `ids` required
  parameter is a string of NPI IDs separated by commas. Returns `400 Bad
  Request` response when not passed or malformed. The positive response will be a
  [Bundle node][bundle] containing child nodes. Example:

  ```bash
  curl https://npi.aidbox.app/practitioner/$batch?ids=1538375811,1447466727
  ```

- `GET /practitioner?q=<term>&_count=<count>`

  Returns either a list of random practitioners or, if a query term was passed,
  a search result. The `q` is an optional query term that searches across
  multiple fields including the last and first names, name prefix and suffix, city,
  state, and zip. The `_count` is an optional integer parameter to limit the number of entries in
  the result (100 by default). Returns a [Bundle node][bundle]. Example:

  ```bash
  curl https://npi.aidbox.app/practitioner?q=david&_count=5
  ```

  If a query term carries several words inside it, for example `foo bar baz`, the result
  logic will concatenate them with `AND`s as the following pseudo-code does:
  `search(foo) AND search(bar) AND search(baz)`

  If words in a query term are separated by `|`, like `foo|bar`, the result
  logic will concatenate them with `OR`s as the following pseudo-code does:
  `search(foo) OR search(bar)`. Operators can be combined — for example, the query
  `foo bar|baz` will work as `search(foo) AND (search(bar) OR search(baz))`

  A query term can have a prefix with colon to guarantee more accurate
  result. For example:

  - `g:David` searches by given name;
  - `p:MD` by prefix;
  - `z:JR` by suffix;
  - `f:Thomson` by family name;
  - `s:TX` by USA state;
  - `c:Rogersville` by city name;
  - `zip:06101` by zip code.

### Organizations

- `GET /organization/<ID>`

  Returns a single [Organization][org] by its ID, or 404 when not found or marked
  as deleted. Example:

  ```bash
  curl https://npi.aidbox.app/organization/1972660348
  ```

- `GET /organization/$batch&ids=<id1,id2,...,idn>`

  Returns multiple organizations at once by their IDs. The `ids` required
  parameter is a string of NPI IDs separated by comma. The result will be a [Bundle
  node][bundle] with child entities. Example:

  ```bash
  curl https://npi.aidbox.app/organization/$batch?ids=1770796096,1700387479
  ```

- `GET /organization?q=<term>&_count=<count>`

  When the `q` parameter is not passed, a query returns random organization nodes. Otherwise, it returns
  a search result made across all organizations by name or address. The result
  will be a [Bundle node][bundle]. The `_count` is an optional integer parameter to limit the
  result. Example:

  ```bash
  curl https://npi.aidbox.app/organization?q=WALMART
  ```

  The system considers multiple words and `|` in a query term the same way as `/practitioner?q=...`
  does.

  A query term can have a prefix with a colon to guarantee more accurate
  result. For example:

  - `n:Walmart` searches by name;
  - `s:TX` by USA state;
  - `c:Rogersville` by city name;
  - `zip:06101` by zip code.

### Other FHIR endpoints

- `GET /metadata`

  Returns a CapabilityStatement object that represents other FHIR
  endpoints. Example:

  ```bash
  curl https://npi.aidbox.app/metadata
  ```

### System status

- `GET /system/env`

  Returns some of ENV variables, e.g. Git commit hash. Example:

  ```bash
  curl https://npi.aidbox.app/system/env
  ```

- `GET /system/updates`

  Returns a list of NPI downloaded and processed files. These files will be
  ignored when trying to process them once again. Example:

  ```bash
  curl https://npi.aidbox.app/system/updates
  ```

- `GET /system/tasks`

  Returns a list of tasks with their schedule info and status. Example:

  ```bash
  curl https://npi.aidbox.app/system/tasks
  ```

- `GET /system/beat`

  Returns a boolean flag that indicates whether a background task processor (also
  known as `beat`) is working or not. Example:

  ```bash
  curl https://npi.aidbox.app/system/beat
  ```

- `GET /system/db`

  Returns the database cache statistics: how many cache blocks are spent on
  different relations (tables, indexes, etc). Also, returns the current cache
  settings. Example:

  ```bash
  curl https://npi.aidbox.app/system/db
  ```

## Install

See the deploy folder and ci3.yaml.

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

* Taxonomy terminology;
* Implement _elements to get only some elements form resource.

## License

Copyright © 2018 Health Samurai

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
