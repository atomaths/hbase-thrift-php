<?php

$GLOBALS['THRIFT_ROOT'] = './thrift/lib/Thrift';

require_once('thrift/src/Thrift.php');

require_once($GLOBALS['THRIFT_ROOT'].'/Type/TMessageType.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Type/TType.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Exception/TException.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Factory/TStringFuncFactory.php');
require_once($GLOBALS['THRIFT_ROOT'].'/StringFunc/TStringFunc.php');
require_once($GLOBALS['THRIFT_ROOT'].'/StringFunc/Core.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Transport/TSocket.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Transport/TBufferedTransport.php');
require_once($GLOBALS['THRIFT_ROOT'].'/Protocol/TBinaryProtocol.php');

require_once('thrift/lib/HBase/Hbase.php');
require_once('thrift/lib/HBase/Types.php');

use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocol;
use Hbase\HbaseClient;
//use Hbase\ColumnDescriptor;
use Hbase\Mutation;
use Hbase\TScan;

$socket = new TSocket('localhost', 9090);
$socket->setSendTimeout(10000); // Ten seconds (too long for production, but this is just a demo ;)
$socket->setRecvTimeout(20000); // Twenty seconds
$transport = new TBufferedTransport( $socket );
$protocol = new TBinaryProtocol( $transport );
$client = new HbaseClient( $protocol );

try {
    $transport->open();
 
    // column family는 character 하나로 주는 게 좋지만, 여기서는 예제로.
    $data = array(new mutation(array('column' => 'columnfamily:column',
                                  'value' => 'column value')));

    $client->mutaterow('table_name', 'rowkey-1', $data, null);

    $transport->close();

} catch (TException $e) {
    print 'TException: '.$e->__toString().' Error: '.$e->getMessage()."\n";
}
