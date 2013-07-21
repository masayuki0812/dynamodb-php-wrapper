<?php

use Aws\DynamoDb\DynamoDbClient;

class DynamoDBWrapper
{
    protected $ddb;

    public function __construct($args)
    {
        $this->client = DynamoDbClient::factory($args);
    }

    public function get($tableName, $key)
    {
        $item = $this->client->getItem(array(
            'TableName' => $tableName,
            'Key' => $key,
        ));
        return $this->convertItem($item['Item']);
    }

    public function batchGet($tableName, $keys)
    {
        $result = $this->client->batchGetItem(array(
            'RequestItems' => array(
                 $tableName => array(
                    'Keys' => $keys,
                 ),
             ),
        ));
        $items = $result->getPath("Responses/{$tableName}");
        return $this->convertItems($items);
    }

    public function query($tableName, $keyConditions, $limit = 100, $index = null)
    {
        $args = array(
            'TableName' => $tableName,
            'KeyConditions' => $keyConditions,
            'ScanIndexForward' => false,
            'Limit' => $limit,
        );
        if (isset($index)) {
            $args['IndexName'] = $index;
        }
        $result = $this->client->query($args);
        return $this->convertItems($result['Items']);
    }

    public function count($tableName, $keyConditions, $index = null)
    {
        $args = array(
            'TableName' => $tableName,
            'KeyConditions' => $keyConditions,
            'Select' => 'COUNT',
        );
        if (isset($index)) {
            $args['IndexName'] = $index;
        }
        $result = $this->client->query($args);
        return $result['Count'];
    }

    public function scan($tableName, $filter, $limit = null)
    {
        $items = $this->client->getIterator('Scan', array(
            'TableName' => $tableName,
            'ScanFilter' => $filter,
        ));
        return $this->convertItems($items);
    }

    public function update($tableName, $key, $update)
    {
        $item = $this->client->updateItem(array(
            'TableName' => $tableName,
            'Key' => $key,
            'AttributeUpdates' => $update,
            'ReturnValues' => 'UPDATED_NEW',
        ));
        return $this->convertItem($item['Attributes']);
    }

    protected function convertItem($item)
    {
        $converted = array();
        foreach ($item as $k => $v) {
            if (array_key_exists('S', $v)) {
                $converted[lcfirst($k)] = $v['S'];
            }
            else if (array_key_exists('N', $v)) {
                $converted[lcfirst($k)] = $v['N'];
            }
            else if (array_key_exists('SS', $v)) {
                $converted[lcfirst($k)] = $v['SS'];
            }
            else {
                throw new Exception('Not implemented type');
            }
        }
        return $converted;
    }

    protected function convertItems($items)
    {
        $converted = array();
        foreach ($items as $item) {
            $converted []= $this->convertItem($item);
        }
        return $converted;
    }
}