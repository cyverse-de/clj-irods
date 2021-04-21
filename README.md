# clj-irods

A Clojure library wrapping clj-jargon and clj-icat-direct for a unified, hopefully performant, interface to IRODS.

## Testing from the REPL

This library is configured to add the path `./repl` to the source file search path in REPL mode, so that developers can
easily test using the Clojure REPL. To use this feature, first create four files: `$HOME/.irods/.qa-db.json` and
`$HOME/.irods/.qa-jargon.json` for QA, and `$HOME/.irods/.prod-db.json` and `$HOME/.irods/.prod-jargon.json` for
production. Note that these files will contain sensitive connection information for iRODS. Be sure to set the file
permissions accordingly. The two database configuration files should look like this:

``` json
{
    "host": "icat.example.org",
    "port": 5432,
    "user": "somedbuser",
    "password": "somedbpassword"
}
```

The two Jargon configuration files should look like this:

``` json
{
    "host": "somehost.example.org",
    "zone": "example",
    "port": "1247",
    "user": "someuser",
    "password": "S0m3-P@$$w0rd",
    "home": "/example/home",
    "resource": "",
    "max-retries": 10,
    "retry-sleep": 1000,
    "use-trash": true
}
```

Once you have the files in place, you can easily prepare the REPL for accessing iRODS by calling either `init-prod` or
`init-qa` from `clj-irods.repl-utils`:

```
user=> (require '[clj-irods.repl-utils :as ru])
nil

user=> (def prod-jargon (ru/init-prod))
#'user/prod-jargon
```

Once the REPL is configured to access iRODS, you can make calls to other functions in clj-irods as usual:

```
user=> (require '[clj-irods.core :refer :all])
nil

user=> (with-irods {:jargon-cfg prod-jargon} irods
        @(permission irods "a" "example" "/example/home/a"))
;; log output omitted
:own
```

During testing, it can sometimes be useful to indicate that you explicitly want to use either the ICAT or
Jargon. Explicitly using the ICAT is easy; all you have to do is omit the Jargon configuration from the call to
`with-irods`:

```
user=> (with-irods {} irods
        @(permission irods "a" "example" "/example/home/a"))
;; log output omitted
:own
```

Explicitly using Jargon requires you to pass a special configuration option to `with-irods`:

```
user=> (with-irods {:jargon-cfg prod-jargon :use-icat false} irods
        @(permission irods "a" "example" "/example/home/a"))
;; log output omitted
:own
```

The `repl` directory is also included in `.gitignore` so that you can add additional namespaces to it without having to
worry very much about accidentally including them in a commit. This can be very helpful if you have an editor that can
interactively run Clojure code.
