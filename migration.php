#!/usr/bin/env php
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

require_once('Hbase.php');
require_once('Types.php');

use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocol;
use Hbase\HbaseClient;
use Hbase\Mutation;
use Hbase\TScan;

$table = 'note';

$fp = @fopen('users.txt', 'r');
if (!$fp) exit;

// $mongos = new MongoClient('localhost'); // XXX: 바꿀곳. mongos 떠있는곳.
// $mongod = new MongoClient('localhost'); // XXX: 바꿀곳. db0,db1,db2 replica sets
$mongos = new MongoClient('mongodb://web3'); // XXX: 바꿀곳. mongos 떠있는곳.
$mongod = new MongoClient('mongodb://db0,db1,db2', array('replicaSet' => 'som')); // XXX: 바꿀곳. db0,db1,db2 replica sets

$socket = new TSocket('localhost', 9090); // XXX: 바꿀곳
$socket->setSendTimeout(30000); // Ten seconds (too long for production, but this is just a demo ;)
$socket->setRecvTimeout(60000); // Twenty seconds
$transport = new TBufferedTransport( $socket );
$protocol = new TBinaryProtocol( $transport );
$client = new HbaseClient( $protocol );

$transport->open();

$cnt = 1;
$total = 1;
while (!feof($fp))
{
    $user_id = trim(fgets($fp, 64));
    if (strlen($user_id) != 24) continue;

    $cur_rev = get_rev($user_id); // 현재 리비전 번호
    if ($cur_rev === false) continue;
    $cur_rev = $cur_rev - 301;
    $cur_rev = $cur_rev < 1 ? 0 : $cur_rev;

    try {
        $where = array('user_id' => $user_id, 'r' => array('$gt' => $cur_rev));
        $cursor = $mongos->note->rev->find($where)->sort(array('r' => -1));
        $cursor->timeout(60000);

        $revisions = align_revision(&$cursor);
        foreach ($revisions as $k => $value) {
            $family = 'r:'.column($k);
            $row = array(new mutation(array('column' => $family,
                                            'value' => json_encode($value))));
            $client->mutaterow($table, $user_id, $row, null);
            $total++;
        }

    } catch (TException $e) {
        print 'TException: '.$e->__toString().' Error: '.$e->getMessage()."\n";
        continue;
    }

    echo $cnt.': '.$user_id."\n";
    $cnt++;
}


fclose($fp);
$transport->close();
$mongod->close(true);
$mongos->close(true);
echo "\nDone!!! (".$total.")\n\n";
exit;


function update_rev($user_id, $r)
{
    global $mongod;

    $where = array('_id' => new MongoId($user_id));
    $set   = array('$set' => array('r' => $r));
    $opts  = array('upsert' => true, 'multiple' => false);
    $mongod->revno_note->rev_no->update($where, $set, $opts);
}

function get_rev($user_id)
{
    global $mongos;
    global $mongod;

    $where = array('_id' => new MongoId($user_id));
    $cursor = $mongod->revno_note->rev_no->find($where)->limit(1);
    $cursor->timeout(30000);
    if ($cursor->count(true) > 0) {
        $row = $cursor->getNext();
        return $row['r'];
    }

    $where  = array('user_id' => $user_id);
    $fields = array('r' => 1);
    $cursor = $mongos->note->rev->find($where)->sort(array('r' => -1))->limit(1); // r를 내림차순으로
    $cursor->timeout(60000);
    if ($cursor->count(true) < 1) {
        return false;
    }
    $row = $cursor->getNext();
    // update_rev($user_id, $row['r']); // XXX: 테스트 시에는 막아두자.
    return $row['r'];
}

function align_revision($cursor)
{
    $ret = array();
    foreach ($cursor as $r) {
        $revno = $r['r'];
        unset($r['_id']); unset($r['user_id']); unset($r['r']);
        $ret[$revno][] = $r;
    }
    return $ret;
}

function column($r)
{
    $r = 1000000000 - $r;
    return str_pad($r, 10, '0', STR_PAD_LEFT);
}
