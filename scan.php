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
 
    $table = 'note';

    $filter = "QualifierFilter(>=, 'binary:1500')"; // greater than 1500
    $scan = new TScan(array(
                    'startRow' => 'rowkey-1',
                    'stopRow' => 'rowkey-2',
                    'filterString' => $filter, 'sortColumns' => true));

    $scanid = $client->scannerOpenWithScan($table, $scan, null);
    $rowresult = $client->scannerGet($scanid);
    // print_r($rowresult);

    echo("\nrow: {$rowresult[0]->row}, cols: \n\n");
    // 위에 TScan에 sortColumns 옵션을 안주면 $rowresult[0]->columns 로 접근.
    $values = $rowresult[0]->sortedColumns;
    // asort($values);
    foreach ($values as $k=>$v) {
        echo("  {$k} => {$v->value}\n");
    }

    $client->scannerClose($scanid);
    $transport->close();

} catch (TException $e) {
    print 'TException: '.$e->__toString().' Error: '.$e->getMessage()."\n";
}
