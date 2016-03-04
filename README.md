# To Csv filter plugin for Embulk

Convert a record to CSV.

## Overview

* **Plugin type**: filter

## Configuration
cf. http://www.embulk.org/docs/built-in.html#csv-formatter-plugin

|name|type|description|required?|
|:---|:---|:---|:---|
|column_name|string|Column name used when converting to single value| `"payload"` by default|
|delimiter|string|Delimiter character such as , for CSV, `"\t"` for TSV, `"|"` or any single-byte character| `,` by default|
|quote|string|The character surrounding a quoted value| `"` by default|
|quote_policy|enum|Policy for quote ( `ALL`, `MINIMAL`, `NONE`) (see below)| `MINIMAL` by default|
|escape|string|Escape character to escape quote character|same with quote default (\*1)|
|header_line|boolean|If true, write the header line with column name at the first line| `false` by default|
|null_string|string|Expression of `NULL` values|empty by default|
|newline|enum|Newline character ( `CRLF`, `LF` or `CR`)| `CRLF` by default|
|newline_in_field|enum|Newline character in each field ( `CRLF`, `LF`, `CR`)| `LF` by default|
|charset|enum|Character encoding (eg. `ISO-8859-1`, `UTF-8`)| `UTF-8` by default|
|default_timezone|string|Time zone of timestamp columns. This can be overwritten for each column using `column_options`| `UTC` by default|
|column_options|hash|See bellow|optional|

(\*1): if quote_policy is `NONE`, quote option is ignored, and default escape is `\`.

The quote_policy option is used to determine field type to quote.

|name|description|
|:---|:---|
| `ALL`|Quote all fields|
| `MINIMAL`|Only quote those fields which contain delimiter, quote or any of the characters in lineterminator|
| `NONE`|Never quote fields. When the delimiter occurs in field, escape with escape char|

The column_options option is a map whose keys are name of columns, and values are configuration with following parameters:

|name|type|description|required?|
|:---|:---|:---|:---|
|timezone|string|Time zone if type of this column is timestamp. If not set, default_timezone is used.|optional|
|format|string|Timestamp format if type of this column is timestamp.| `%Y-%m-%d %H:%M:%S.%6N %z` by default|

## Why does this need?
Some output plugin cannot use a formatter plugin, because they are not inherited `FileOutputPlugin`, but sometimes they need formatters.
In that case, this plugin is useful. For example, [embulk-output-bigquery](https://github.com/sonots/embulk-output-bigquery/blob/ruby/README.md#formatter-performance-issue)

## Example

```yaml
filters:
  - type: to_csv
    column_name: payload
    delimiter: "\t"
    newline: CRLF
    newline_in_field: LF
    charset: UTF-8
    quote_policy: MINIMAL
    quote: '"'
    escape: "\\"
    null_string: "\\N"
    default_timezone: 'UTC'
    column_options:
      time: {format: '%Y-%m-%d %H:%M:%S', timezone: 'America/Los_Angeles'}

```

## Run the example

```
$ ./gradlew classpath
$ embulk run example/config.yml -Ilib
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
