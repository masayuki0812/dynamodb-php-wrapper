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
            'Key' => $this->convertAttributes($key),
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

        $ddbKeys = array();
        foreach ($keys as $key) {
            $ddbKeys[] = $this->convertAttributes($key);
        }

        while (count($ddbKeys) > 0) {
            $targetKeys = array_splice($ddbKeys, 0, 100);

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
                $ddbKeys = array_merge($ddbKeys, $unprocessedKeys);
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
            'KeyConditions' => $this->convertConditions($keyConditions),
            'ScanIndexForward' => true,
            'Limit' => 100,
        );
        if (isset($options['ScanIndexForward'])) {
            $args['ScanIndexForward'] = $options['ScanIndexForward'];
        }
        if (isset($options['IndexName'])) {
            $args['IndexName'] = $options['IndexName'];
        }
        if (isset($options['Limit'])) {
            $args['Limit'] = $options['Limit']+0;
        }
        if (isset($options['ExclusiveStartKey'])) {
            $args['ExclusiveStartKey'] = $this->convertAttributes($options['ExclusiveStartKey']);
        }
        $result = $this->client->query($args);
        return $this->convertItems($result['Items']);
    }

    public function count($tableName, $keyConditions, $options = array())
    {
        $args = array(
            'TableName' => $tableName,
            'KeyConditions' => $this->convertConditions($keyConditions),
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
            'ScanFilter' => $this->convertConditions($filter),
        ));
        return $this->convertItems($items);
    }

    public function put($tableName, $item, $expected = array())
    {
        $args = array(
            'TableName' => $tableName,
            'Item' => $this->convertAttributes($item),
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
        return $this->batchWrite('PutRequest', $tableName, $items);
    }

    public function update($tableName, $key, $update, $expected = array())
    {
        $args = array(
            'TableName' => $tableName,
            'Key' => $this->convertAttributes($key),
            'AttributeUpdates' => $this->convertUpdateAttributes($update),
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
            'Key' => $this->convertAttributes($key),
            'ReturnValues' => 'ALL_OLD',
        );
        $result = $this->client->deleteItem($args);
        return $this->convertItem($result['Attributes']);
    }

    public function batchDelete($tableName, $keys)
    {
        return $this->batchWrite('DeleteRequest', $tableName, $keys);
    }

    protected function batchWrite($requestType, $tableName, $items)
    {
        $entityKeyName = ($requestType === 'PutRequest' ? 'Item' : 'Key');

        $requests = array();
        foreach ($items as $item) {
            $requests[] = array(
                $requestType => array(
                    $entityKeyName => $this->convertAttributes($item)
                )
            );
        }

        while (count($requests) > 0) {
            $targetRequests = array_splice($requests, 0, 25);

            $result = $this->client->batchWriteItem(array(
                'RequestItems' => array(
                    $tableName => $targetRequests
                ),
            ));

            // if some items not processed, try again as next request
            $unprocessedRequests = $result->getPath("UnprocessedItems/{$tableName}");
            if (count($unprocessedRequests) > 0) {
                $requests = array_merge($requests, $unprocessedRequests);
            }
        }

        return true;
    }

    public function createTable($tableName, $hashKey, $rangeKey = null, $secondaryIndices = null) {

        $attributeDefinitions = array();
        $keySchema = array();

        // HashKey
        $hashKeyComponents = explode('::', $hashKey);
        if (count($hashKeyComponents) < 2) {
            $hashKeyComponents[1] = 'S';
        }
        $hashKeyName = $hashKeyComponents[0];
        $hashKeyType = $hashKeyComponents[1];
        $attributeDefinitions []= array('AttributeName' => $hashKeyName, 'AttributeType' => $hashKeyType);
        $keySchema[] = array('AttributeName' => $hashKeyName, 'KeyType' => 'HASH');

        // RangeKey
        if (isset($rangeKey)) {
            $rangeKeyComponents = explode('::', $rangeKey);
            if (count($rangeKeyComponents) < 2) {
                $rangeKeyComponents[1] = 'S';
            }
            $rangeKeyName = $rangeKeyComponents[0];
            $rangeKeyType = $rangeKeyComponents[1];
            $attributeDefinitions[] = array('AttributeName' => $rangeKeyName, 'AttributeType' => $rangeKeyType);
            $keySchema[] = array('AttributeName' => $rangeKeyName, 'KeyType' => 'RANGE');
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

    public function deleteTable($tableName)
    {
        $this->client->deleteTable(array('TableName' => $tableName));
        $this->client->waitUntilTableNotExists(array('TableName' => $tableName));
    }

    public function emptyTable($table) {
        // Get table info
        $result = $this->client->describeTable(array('TableName' => $table));
        $keySchema = $result['Table']['KeySchema'];
        foreach ($keySchema as $schema) {
            if ($schema['KeyType'] === 'HASH') {
                $hashKeyName = $schema['AttributeName'];
            }
            else if ($schema['KeyType'] === 'RANGE') {
                $rangeKeyName = $schema['AttributeName'];
            }
        }

        // Delete items in the table
        $scan = $this->client->getIterator('Scan', array('TableName' => $table));
        foreach ($scan as $item) {
            // set hash key
            $hashKeyType = array_key_exists('S', $item[$hashKeyName]) ? 'S' : 'N';
            $key = array(
                $hashKeyName => array($hashKeyType => $item[$hashKeyName][$hashKeyType]),
            );
            // set range key if defined
            if (isset($rangeKeyName)) {
                $rangeKeyType = array_key_exists('S', $item[$rangeKeyName]) ? 'S' : 'N';
                $key[$rangeKeyName] = array($rangeKeyType => $item[$rangeKeyName][$rangeKeyType]);
            }
            $this->client->deleteItem(array(
                'TableName' => $table,
                'Key' => $key
            ));
        }
    }

    protected function convertAttributes($targets)
    {
        $newTargets = array();
        foreach ($targets as $k => $v) {
            $attrComponents = explode('::', $k);
            if (count($attrComponents) < 2) {
                $attrComponents[1] = 'S';
            }
            $newTargets[$attrComponents[0]] = array($attrComponents[1] => $v);
        }
        return $newTargets;
    }

    protected function convertUpdateAttributes($targets)
    {
        $newTargets = array();
        foreach ($targets as $k => $v) {
            $attrComponents = explode('::', $k);
            if (count($attrComponents) < 2) {
                $attrComponents[1] = 'S';
            }
            $newTargets[$attrComponents[0]] = array(
                'Action' => $v[0],
                'Value' => array($attrComponents[1] => $v[1]),
            );
        }
        return $newTargets;
    }

    protected function convertConditions($conditions)
    {
        $ddbConditions = array();
        foreach ($conditions as $k => $v) {
            // Get attr name and type
            $attrComponents = explode('::', $k);
            if (count($attrComponents) < 2) {
                $attrComponents[1] = 'S';
            }
            $attrName = $attrComponents[0];
            $attrType = $attrComponents[1];

            // Get ComparisonOperator and value
            if ( ! is_array($v)) {
                $v = array('EQ', $v);
            }
            $comparisonOperator = $v[0];
            $value = count($v) > 1 ? $v[1] : null;

            // Get AttributeValueList
            if ($v[0] === 'BETWEEN') {
                if (count($value) !== 2) {
                    throw new Exception("Require 2 values as array for BETWEEN");
                }
                $attributeValueList = array(
                    array($attrType => $value[0]),
                    array($attrType => $value[1])
                );
            } else if ($v[0] === 'IN') {
                $attributeValueList = array();
                foreach ($value as $v) {
                    $attributeValueList[] = array($attrType => $v);
                }
            } else if ($v[0] === 'NOT_NULL' || $v[0] === 'NULL') {
                $attributeValueList = null;
            } else {
                $attributeValueList = array(
                    array($attrType => $value),
                );
            }

            // Constract key condition for DynamoDB
            $ddbConditions[$attrName] = array(
                'AttributeValueList' => $attributeValueList,
                'ComparisonOperator' => $comparisonOperator
            );
        }

        return $ddbConditions;
    }

    protected function convertItem($item)
    {
        if (empty($item)) return null;

        $converted = array();
        foreach ($item as $k => $v) {
            if (isset($v['S'])) {
                $converted[$k] = $v['S'];
            }
            else if (isset($v['SS'])) {
                $converted[$k] = $v['SS'];
            }
            else if (isset($v['N'])) {
                $converted[$k] = $v['N'];
            }
            else if (isset($v['NS'])) {
                $converted[$k] = $v['NS'];
            }
            else if (isset($v['B'])) {
                $converted[$k] = $v['B'];
            }
            else if (isset($v['BS'])) {
                $converted[$k] = $v['BS'];
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