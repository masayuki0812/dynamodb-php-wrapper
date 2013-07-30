<?php

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\ConditionalCheckFailedException;

class DynamoDBWrapper
{
    protected $ddb;

    public function __construct($args)
    {
        $this->client = DynamoDbClient::factory($args);
    }

    public function get($tableName, $key, $options = array())
    {
        $args = array(
            'TableName' => $tableName,
            'Key' => $key,
        );
        if (isset($options['ConsistentRead'])) {
            $args['ConsistentRead'] = $options['ConsistentRead'];
        }
        $item = $this->client->getItem($args);
        return $this->convertItem($item['Item']);
    }

    public function batchGet($tableName, $keys, $options = array())
    {
        $results = array();

        while (count($keys) > 0) {
            $targetKeys = array_splice($keys, 0, 100);

            $result = $this->client->batchGetItem(array(
                'RequestItems' => array(
                     $tableName => array(
                        'Keys' => $targetKeys,
                     ),
                 ),
            ));
            $items = $result->getPath("Responses/{$tableName}");
            $results = array_merge($results, $this->convertItems($items));

            // if some keys not processed, try again as next request
            $unprocessedKeys = $result->getPath("UnprocessedKeys/{$tableName}");
            if (count($unprocessedKeys) > 0) {
                $keys = array_merge($keys, $unprocessedKeys);
            }
        }

        if (isset($options['Order'])) {
            if ( ! isset($options['Order']['Key'])) {
                throw new Exception("Order option needs 'Key'.");
            }
            $key = $options['Order']['Key'];

            if (isset($options['Order']['Forward']) && !$options['Order']['Forward']) {
                $vals = array('b', 'a');
            } else {
                $vals = array('a', 'b');
            }

            $f = 'return ($'.$vals[0].'[\''.$key.'\'] - $'.$vals[1].'[\''.$key.'\']);';
            usort($results, create_function('$a,$b',$f));
        }

        return $results;
    }

    public function query($tableName, $keyConditions, $options = array())
    {
        $args = array(
            'TableName' => $tableName,
            'KeyConditions' => $keyConditions,
            'ScanIndexForward' => false,
            'Limit' => 100,
        );
        if (isset($options['IndexName'])) {
            $args['IndexName'] = $options['IndexName'];
        }
        if (isset($options['Limit'])) {
            $args['Limit'] = $options['Limit']+0;
        }
        if (isset($options['ExclusiveStartKey'])) {
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
        if (isset($options['IndexName'])) {
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

    public function put($tableName, $item, $expected = array())
    {
        $args = array(
            'TableName' => $tableName,
            'Item' => $item,
        );
        if (!empty($expected)) {
            $item['Expected'] = $expected;
        }
        // Put and catch exception when ConditionalCheckFailed
        try {
            $item = $this->client->putItem($args);
        }
        catch (ConditionalCheckFailedException $e) {
            return false;
        }
        return true;
    }

    public function batchPut($tableName, $items)
    {
        $unprocessedRequests = array();

        while (count($items) > 0) {
            $requests = array();

            if (count($unprocessedRequests) > 0) {
                $requests = array_merge($requests, $unprocessedRequests);
            }

            $targetItems = array_splice($items, 0, 25 - count($requests));
            foreach ($targetItems as $targetItem) {
                $requests[] = array(
                    'PutRequest' => array(
                        'Item' => $targetItem
                    )
                );
            }

            $result = $this->client->batchWriteItem(array(
                'RequestItems' => array(
                    $tableName => $requests,
                ),
            ));

            // if some items not processed, try again as next request
            $unprocessedRequests = $result->getPath("UnprocessedItems/{$tableName}");
        }

        return true;
    }

    public function update($tableName, $key, $update, $expected = array())
    {
        $args = array(
            'TableName' => $tableName,
            'Key' => $key,
            'AttributeUpdates' => $update,
            'ReturnValues' => 'UPDATED_NEW',
        );
        if (!empty($expected)) {
            $item['Expected'] = $expected;
        }
        // Put and catch exception when ConditionalCheckFailed
        try {
            $item = $this->client->updateItem($args);
        }
        catch (ConditionalCheckFailed $e) {
            return null;
        }
        return $this->convertItem($item['Attributes']);
    }

    public function delete($tableName, $key)
    {
        $args = array(
            'TableName' => $tableName,
            'Key' => $key,
            'ReturnValues' => 'ALL_OLD',
        );
        $result = $this->client->deleteItem($args);
        return $this->convertItem($result['Attributes']);
    }

    public function batchDelete($tableName, $keys)
    {
        $unprocessedRequests = array();

        while (count($keys) > 0) {
            $requests = array();

            if (count($unprocessedRequests) > 0) {
                $requests = array_merge($requests, $unprocessedRequests);
            }

            $targetKeys = array_splice($keys, 0, 25 - count($requests));
            foreach ($targetKeys as $targetKey) {
                $requests[] = array(
                    'DeleteRequest' => array(
                        'Key' => $targetKey
                    )
                );
            }

            $result = $this->client->batchWriteItem(array(
                'RequestItems' => array(
                    $tableName => $requests,
                ),
            ));

            // if some items not processed, try again as next request
            $unprocessedRequests = $result->getPath("UnprocessedItems/{$tableName}");
        }

        return true;
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
        if (empty($item)) return null;

        $converted = array();
        foreach ($item as $k => $v) {
            if (isset($v['S'])) {
                $converted[$k] = $v['S'];
            }
            else if (isset($v['N'])) {
                $converted[$k] = $v['N'];
            }
            else if (isset($v['SS'])) {
                $converted[$k] = $v['SS'];
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