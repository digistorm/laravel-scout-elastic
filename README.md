# Laravel Scout Elasticsearch Driver

Forked from https://github.com/ErickTamayo/laravel-scout-elastic

This fork adds the following

- Extended ElasticsearchEngine to handle read & write index aliases
- Raise ElasticsearchErrorsEvent when errors are detected in the update/delete response
- Changed dependency on `elasticsearch/elasticsearch` to 7.9 as this is the version currently provided by AWS
