<?php

namespace Tamayo\LaravelScoutElastic\Engines;

use Laravel\Scout\Builder;
use Laravel\Scout\Engines\Engine;
use Elasticsearch\Client as Elastic;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Arr;
use Tamayo\LaravelScoutElastic\ElasticsearchErrorsEvent;

class ElasticsearchEngine extends Engine
{
    const INDEX_READ = 'read';
    const INDEX_WRITE = 'write';

    /**
     * Elastic client.
     *
     * @var Elastic
     */
    protected $elastic;

    /**
     * Index where searches will be made.
     *
     * @var string
     */
    protected $readIndexBaseName;

    /**
     * Index where the models will be saved.
     *
     * @var string
     */
    protected $writeIndexBaseName;

    /**
     * Create a new engine instance.
     *
     * @param  \Elasticsearch\Client  $elastic
     * @param $readIndexBaseName
     * @param $writeIndexBaseName
     *
     * @return void
     */
    public function __construct(Elastic $elastic, $readIndexBaseName = self::INDEX_READ, $writeIndexBaseName = self::INDEX_READ)
    {
        $this->elastic = $elastic;
        $this->readIndexBaseName = config('scout.elasticsearch.index-read') ?? $readIndexBaseName;
        $this->writeIndexBaseName = config('scout.elasticsearch.index-write') ?? $writeIndexBaseName;
    }

    /**
     * Update the given model in the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function update($models)
    {
        if ($models->isEmpty()) {
            return;
        }

        $params['body'] = [];

        $models->each(function ($model) use (&$params) {
            $params['body'][] = [
                'update' => [
                    '_id' => $model->getScoutKey(),
                    '_index' => $this->modelIndexName($model, self::INDEX_WRITE),
                ]
            ];
            $params['body'][] = [
                'doc' => $model->toSearchableArray(),
                'doc_as_upsert' => true
            ];
        });

        $result = $this->elastic->bulk($params);

        $this->checkResultForErrors('update', $result, $params);
    }

    /**
     * Remove the given model from the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function delete($models)
    {
        $params['body'] = [];

        $models->each(function ($model) use (&$params) {
            $params['body'][] = [
                'delete' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->modelIndexName($model, self::INDEX_WRITE),
                ]
            ];
        });

        $result = $this->elastic->bulk($params);

        $this->checkResultForErrors('delete', $result, $params);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @return mixed
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder, array_filter([
            'numericFilters' => $this->filters($builder),
            'size' => $builder->limit,
        ]));
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
    public function paginate(Builder $builder, $perPage, $page)
    {
        $result = $this->performSearch($builder, [
            'numericFilters' => $this->filters($builder),
            'from' => (($page * $perPage) - $perPage),
            'size' => $perPage,
        ]);

        $result['nbPages'] = $this->getTotalCount($result) / $perPage;

        return $result;
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  array  $options
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        $params = [
            'index' => $this->modelIndexName($builder->model, self::INDEX_READ),
            'body' => [
                'query' => [
                    'bool' => [
                        'must' => [['query_string' => ['query' => "*{$builder->query}*"]]]
                    ]
                ]
            ]
        ];

        if ($sort = $this->sort($builder)) {
            $params['body']['sort'] = $sort;
        }

        if (isset($options['from'])) {
            $params['body']['from'] = $options['from'];
        }

        if (isset($options['size'])) {
            $params['body']['size'] = $options['size'];
        }

        if (isset($options['numericFilters']) && count($options['numericFilters'])) {
            $params['body']['query']['bool']['must'] = array_merge(
                $params['body']['query']['bool']['must'],
                $options['numericFilters']
            );
        }

        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->elastic,
                $builder->query,
                $params
            );
        }

        return $this->elastic->search($params);
    }

    /**
     * Get the filter array for the query.
     *
     * @param  Builder  $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        return collect($builder->wheres)->map(function ($value, $key) {
            if (is_array($value)) {
                return ['terms' => [$key => $value]];
            }

            return ['match_phrase' => [$key => $value]];
        })->values()->all();
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param  mixed  $results
     * @return \Illuminate\Support\Collection
     */
    public function mapIds($results)
    {
        return collect($results['hits']['hits'])->pluck('_id')->values();
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return Collection
     */
    public function map(Builder $builder, $results, $model)
    {
        if ($this->getTotalCount($results) === 0) {
            return $model->newCollection();
        }

        $keys = collect($results['hits']['hits'])->pluck('_id')->values()->all();

        $modelIdPositions = array_flip($keys);

        return $model->getScoutModelsByIds(
            $builder,
            $keys
        )->filter(function ($model) use ($keys) {
            return in_array($model->getScoutKey(), $keys);
        })->sortBy(function ($model) use ($modelIdPositions) {
            return $modelIdPositions[$model->getScoutKey()];
        })->values();
    }

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param  mixed  $results
     * @return int
     */
    public function getTotalCount($results)
    {
        $total = Arr::get($results, 'hits.total');
        // ES version 7+
        if (is_array($total)) {
            $total = $total['value'];
        }

        return $total;
    }

    /**
     * Flush all of the model's records from the engine.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return void
     */
    public function flush($model)
    {
        $model->newQuery()
            ->orderBy($model->getKeyName())
            ->unsearchable();
    }

    /**
     * Generates the sort if theres any.
     *
     * @param  Builder $builder
     * @return array|null
     */
    protected function sort($builder)
    {
        if (count($builder->orders) == 0) {
            return null;
        }

        return collect($builder->orders)->map(function ($order) {
            return [$order['column'] => $order['direction']];
        })->toArray();
    }

    /**
     * @param string $indexMode
     * @param \Illuminate\Database\Eloquent\Model $model
     * @param array $mapping
     * @return array
     */
    public function putMapping($indexMode, Model $model, array $mapping)
    {
        $bodyArray = [
            'doc' => [
                // Add dynamic date formats to match the provided date values
                'dynamic_date_formats' => ['yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'],
            ],
        ];

        // If properties is set in the mapping array, set the full body array.
        if (isset($mapping['properties'])) {
            $bodyArray['doc'] = $bodyArray['doc'] + $mapping;
        } else {
            $bodyArray['doc']['properties'] = $mapping;
        }

        $params = [
            'index' => $this->modelIndexName($model, $indexMode),
            'body' => $bodyArray,
        ];

        return $this->elastic->indices()->putMapping($params);
    }

    /**
     * Get an index name for a particular model instance e.g. 'edustack_read_1_crm_leads'
     * @param \Illuminate\Database\Eloquent\Model $model
     * @param string $indexMode
     * @return string
     * @throws \Exception
     */
    public function modelIndexName(Model $model, $indexMode = self::INDEX_READ)
    {
        switch ($indexMode) {
            case self::INDEX_READ : return $this->readIndexBaseName . '_' . $model->searchableAs();
            case self::INDEX_WRITE : return $this->writeIndexBaseName . '_' . $model->searchableAs();
        }

        throw new \Exception('Invalid index type: ' . $indexMode);
    }

    /**
     * Raise an event if an Elasticsearch error is encountered so we can see them after
     *
     * @param $operation
     * @param $result
     * @param $params
     */
    protected function checkResultForErrors($operation, $result, $params)
    {
        if (isset($result['errors']) && true === $result['errors']) {
            $errors = [];
            foreach ($result['items'] as $item) {

                if (isset($item['update']['error'])) {
                    if (isset($item['update']['error']['type'], $item['update']['error']['reason'])) {
                        $message = sprintf('[%s] %s', $item['update']['error']['type'], $item['update']['error']['reason']);
                    } else {
                        $message = json_encode($item['update'], JSON_PRETTY_PRINT);
                    }

                    $errors[] = [
                        'index' => $item['update']['_index'],
                        'key' => $item['update']['_id'],
                        'message' => $message,
                    ];
                }
            }

            event(new ElasticsearchErrorsEvent($operation, $errors, $params));
        }
    }
}
