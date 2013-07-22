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

    public function query($tableName, $keyConditions, $options = array())
    {
        $args = array(
            'TableName' => $tableName,
            'KeyConditions' => $keyConditions,
            'ScanIndexForward' => false,
            'Limit' => 100,
        );
        if (array_key_exists('IndexName', $options)) {
            $args['IndexName'] = $options['IndexName'];
        }
        if (array_key_exists('Limit', $options)) {
            $args['Limit'] = $options['Limit']+0;
        }
        if (array_key_exists('ExclusiveStartKey', $options)) {
            $args['ExclusiveStartKey'] = $options['ExclusiveStartKey'];
        }
        $result = $this->client->query($args);
        return $this->convertItems($result['Items']);
    }

    public function count($tableName, $keyConditions, $options = array())
    {
        $args = array(
            'TableName' => $tableName,
            'KeyConditions' => $keyConditions,
            'Select' => 'COUNT',
        );
        if (array_key_exists('IndexName', $options)) {
            $args['IndexName'] = $options['IndexName'];
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

    public function createTable($tableName, $hashKeyName, $hashKeyType, $rangeKeyName = null, $rangeKeyType = null, $secondaryIndices = null) {

        $attributeDefinitions = array();
        $keySchema = array();

        // HashKey
        $attributeDefinitions []= array('AttributeName' => $hashKeyName, 'AttributeType' => $hashKeyType);
        $keySchema []= array('AttributeName' => $hashKeyName, 'KeyType' => 'HASH');

        // RangeKey
        if (isset($rangeKeyName)) {
            $attributeDefinitions []= array('AttributeName' => $rangeKeyName, 'AttributeType' => $rangeKeyType);
            $keySchema []= array('AttributeName' => $rangeKeyName, 'KeyType' => 'RANGE');
        }

        // Generate Args
        $args = array(
            'TableName' => $tableName,
            'AttributeDefinitions' => $attributeDefinitions,
            'KeySchema' => $keySchema,
            'ProvisionedThroughput' => array(
                'ReadCapacityUnits'  => 1,
                'WriteCapacityUnits' => 1
            )
        );

        // Set Local Secondary Index if needed
        if (isset($secondaryIndices)) {
            $LSI = array();
            foreach ($secondaryIndices as $si) {
                $LSI []= array(
                    'IndexName' => $si['name'].'Index',
                    'KeySchema' => array(
                        array('AttributeName' => $hashKeyName, 'KeyType' => 'HASH'),
                        array('AttributeName' => $si['name'], 'KeyType' => 'RANGE')
                    ),
                    'Projection' => array(
                        'ProjectionType' => $si['projection_type']
                    ),
                );
                $attributeDefinitions []= array('AttributeName' => $si['name'], 'AttributeType' => $si['type']);
            }
            $args['LocalSecondaryIndexes'] = $LSI;
            $args['AttributeDefinitions'] = $attributeDefinitions;
        }

        $this->client->createTable($args);
        $this->client->waitUntilTableExists(array('TableName' => $tableName));
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