<?php

namespace Tamayo\LaravelScoutElastic;

use Illuminate\Queue\SerializesModels;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Broadcasting\InteractsWithSockets;

class ElasticsearchErrorsEvent
{
    use Dispatchable, InteractsWithSockets, SerializesModels;

    protected $operation;

    protected $errors = [];

    protected $params = [];

    /**
     * Create a new event instance.
     * @param $operation
     * @param array $errors
     */
    public function __construct($operation, array $errors, array $params)
    {
        $this->operation = $operation;
        $this->errors = $errors;
        $this->params = $params;
    }

    public function getOperation()
    {
        return $this->operation;
    }

    public function getErrors()
    {
        return $this->errors;
    }

    public function getParams()
    {
        return $this->params;
    }
}
