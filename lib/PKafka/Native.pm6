=begin license

Copyright (c) 2016 Maxim Noah Khailo, All Rights Reserved

This file is part of PKafka.

PKafka is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

PKafka is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with PKafka.  If not, see <http://www.gnu.org/licenses/>.
use NativeCall;

=end license

use NativeCall :ALL;
unit module PKafka;

class PKafka::X::DidNotSpecifyBrokers is Exception
{
    method message {"No brokers specified"}
};

class PKafka::X::DidNotProvideValidBrokers is Exception
{
    has $.brokers;
    method message {"No valid brokers specified $.brokers"}
};

my $errno := cglobal(Str, 'errno', int);

enum rd_kafka_type_t(
    RD_KAFKA_PRODUCER => 0,
    RD_KAFKA_CONSUMER => 1
);

enum errno_errors(
    ETIMEDOUT=>110,
    ENOENT=>2
);

enum rd_kafka_resp_err_t(
    RD_KAFKA_RESP_ERR__BEGIN => -200,     
    RD_KAFKA_RESP_ERR__BAD_MSG => -199,  
    RD_KAFKA_RESP_ERR__BAD_COMPRESSION => -198,
    RD_KAFKA_RESP_ERR__DESTROY => -197,   
    RD_KAFKA_RESP_ERR__FAIL => -196,     
    RD_KAFKA_RESP_ERR__TRANSPORT => -195,
    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE => -194,
    RD_KAFKA_RESP_ERR__RESOLVE => -193,  
    RD_KAFKA_RESP_ERR__MSG_TIMED_OUT => -192,
    RD_KAFKA_RESP_ERR__PARTITION_EOF => -191,
    RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION => -190,
    RD_KAFKA_RESP_ERR__FS => -189,       
    RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC => -188,
    RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN => -187,
    RD_KAFKA_RESP_ERR__INVALID_ARG => -186, 
    RD_KAFKA_RESP_ERR__TIMED_OUT => -185,  
    RD_KAFKA_RESP_ERR__QUEUE_FULL => -184, 
    RD_KAFKA_RESP_ERR__ISR_INSUFF => -183, 
    RD_KAFKA_RESP_ERR__NODE_UPDATE => -182, 
    RD_KAFKA_RESP_ERR__END => -100,      
    RD_KAFKA_RESP_ERR_UNKNOWN => -1,
    RD_KAFKA_RESP_ERR_NO_ERROR => 0,
    RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE => 1,
    RD_KAFKA_RESP_ERR_INVALID_MSG => 2,
    RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => 3,
    RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE => 4,
    RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE => 5,
    RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION => 6,
    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT => 7,
    RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE => 8,
    RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE => 9,
    RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE => 10,
    RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH => 11,
    RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE => 12,
    RD_KAFKA_RESP_ERR_OFFSETS_LOAD_IN_PROGRESS => 14,
    RD_KAFKA_RESP_ERR_CONSUMER_COORDINATOR_NOT_AVAILABLE => 15,
    RD_KAFKA_RESP_ERR_NOT_COORDINATOR_FOR_CONSUMER => 16
);

our sub rd_kafka_version() returns int32 is native('rdkafka', v1) { * }; 
our sub rd_kafka_version_str() returns Str is native('rdkafka', v1) { * }; 

#const char* rd_kafka_err2str(rd_kafka_resp_err_t)
our sub rd_kafka_err2str(int32) returns Str is native('rdkafka', v1) { * };
our sub _rd_kafka_errno2err (int32) returns int32 is native('rdkafka', v1) is symbol('rd_kafka_errno2err') { * };
our sub rd_kafka_errno2err (int $errnox) returns rd_kafka_resp_err_t 
{
    rd_kafka_resp_err_t(_rd_kafka_errno2err($errnox))
}

our sub errno2str() { my $e = $errno; rd_kafka_err2str(rd_kafka_errno2err($e)); }

#void rd_kafka_message_destroy (rd_kafka_message_t *rkmessage);
our sub rd_kafka_message_destroy(Pointer) is native('rdkafka', v1) { * };

#rd_kafka_conf_t *rd_kafka_conf_new (void);
our sub rd_kafka_conf_new returns Pointer is native('rdkafka', v1) { * };

#void rd_kafka_conf_destroy (rd_kafka_conf_t *conf);
our sub rd_kafka_conf_destroy(Pointer) is native('rdkafka', v1) { * };

#rd_kafka_conf_t *rd_kafka_conf_dup (const rd_kafka_conf_t *conf);
our sub rd_kafka_conf_dup(Pointer) returns Pointer is native('rdkafka', v1) { * };

enum rd_kafka_conf_res_t(
    RD_KAFKA_CONF_UNKNOWN => -2,
    RD_KAFKA_CONF_INVALID => -1,
    RD_KAFKA_CONF_OK => 0 
);

#rd_kafka_conf_res_t rd_kafka_conf_set (rd_kafka_conf_t *conf, const char *name, const char *value, char *errstr, size_t errstr_size);
our sub _rd_kafka_conf_set(Pointer, Str, Str, CArray[uint8], uint64) returns int32 is native('rdkafka', v1) is symbol('rd_kafka_conf_set'){ * };

our sub array-to-str(CArray[uint8] $as) returns Str 
{
    $as.map( -> $a { sprintf("%c", $a)}).join('');
}

our sub array-to-str-n(CArray[uint8] $as, Int $len) returns Str 
{
    (for ^$len -> $i { my uint8 $c = $as[$i]; sprintf("%c", $c)}).join('');
}

our sub array-to-blob(CArray[uint8] $as, Int $len) returns Buf 
{
    my $b = Buf.new;
    for ^$len -> $i { $b[$i] = $as[$i];}
    return $b;
}

our sub rd_kafka_conf_set(Pointer $conf, Str $name, Str $value) 
{
    my uint64 $errstr_size = 512;
    my @errstr := CArray[uint8].new;
    @errstr[$errstr_size] = 0;
    my $res = rd_kafka_conf_res_t(_rd_kafka_conf_set($conf, $name, $value, @errstr, $errstr_size));
    ($res, array-to-str(@errstr));
}

#rd_kafka_topic_conf_t *rd_kafka_topic_conf_new (void);
our sub rd_kafka_topic_conf_new returns Pointer is native('rdkafka', v1) { * };

#rd_kafka_topic_conf_t *rd_kafka_topic_conf_dup (const rd_kafka_topic_conf_t *conf)
our sub rd_kafka_topic_conf_dup(Pointer) returns Pointer is native('rdkafka', v1) { * };

#void rd_kafka_topic_conf_destroy (rd_kafka_topic_conf_t *topic_conf);
our sub rd_kafka_topic_conf_destroy(Pointer) is native('rdkafka', v1) { * };

#rd_kafka_conf_res_t rd_kafka_topic_conf_set (rd_kafka_topic_conf_t *conf, const char *name, const char *value, char *errstr, size_t errstr_size); 
our sub _rd_kafka_topic_conf_set(Pointer, Str, Str, CArray[uint8], uint64) returns int32 is native('rdkafka', v1) is symbol('rd_kafka_topic_conf_set'){ * };
our sub rd_kafka_topic_conf_set(Pointer $conf, Str $name, Str $value) 
{
    my uint64 $errstr_size = 512;
    my @errstr := CArray[uint8].new;
    @errstr[$errstr_size] = 0;
    my $res = rd_kafka_conf_res_t(_rd_kafka_topic_conf_set($conf, $name, $value, @errstr, $errstr_size));
    ($res, array-to-str(@errstr));
}


#rd_kafka_t *rd_kafka_new (rd_kafka_type_t type, rd_kafka_conf_t *conf, char *errstr, size_t errstr_size);
our sub _rd_kafka_new(int32, Pointer, CArray[uint8], uint64) returns Pointer is native('rdkafka', v1) is symbol('rd_kafka_new') { * }

our sub rd_kafka_new(rd_kafka_type_t $type, Pointer $config) 
{
    my uint64 $errstr_size = 512;
    my @errstr := CArray[uint8].new;
    @errstr[$errstr_size] = 0;
    my $res = _rd_kafka_new($type, $config, @errstr, $errstr_size); 
    ($res, array-to-str(@errstr));
}

#void        rd_kafka_destroy (rd_kafka_t *rk);
our sub rd_kafka_destroy(Pointer) is native('rdkafka', v1) { * }

#const char *rd_kafka_name (const rd_kafka_t *rk);
our sub rd_kafka_name(Pointer) returns Str is native('rdkafka', v1) { * }


#rd_kafka_topic_t *rd_kafka_topic_new (rd_kafka_t *rk, const char *topic, rd_kafka_topic_conf_t *conf);
our sub rd_kafka_topic_new(Pointer, Str, Pointer) returns Pointer is native('rdkafka', v1) { * }

#void rd_kafka_topic_destroy (rd_kafka_topic_t *rkt);
our sub rd_kafka_topic_destroy(Pointer) is native('rdkafka', v1) { * } 

#const char *rd_kafka_topic_name (const rd_kafka_topic_t *rkt);
our sub rd_kafka_topic_name(Pointer) returns Str is native('rdkafka', v1) { * }

#int rd_kafka_consume_start (rd_kafka_topic_t *rkt, int32_t partition, int64_t offset);
our sub rd_kafka_consume_start(Pointer, int32, int64) returns int32 is native('rdkafka', v1) { * }

#int rd_kafka_consume_stop (rd_kafka_topic_t *rkt, int32_t partition);
our sub rd_kafka_consume_stop(Pointer, int32) returns int32 is native('rdkafka', v1) { * }

#rd_kafka_resp_err_t rd_kafka_offset_store (rd_kafka_topic_t *rkt, int32_t partition, int64_t offset);
our sub _rd_kafka_offset_store(Pointer, int32, int64) returns int32 is native('rdkafka', v1) is symbol('rd_kafka_offset_store') { * }
our sub rd_kafka_offset_store(Pointer $topic, int32 $partition, int64 $offset) returns rd_kafka_resp_err_t
{
    rd_kafka_resp_err_t(_rd_kafka_offset_store($topic, $partition, $offset));
}

#int rd_kafka_brokers_add (rd_kafka_t *rk, const char *brokerlist);
our sub rd_kafka_brokers_add(Pointer, Str) returns int32 is native('rdkafka', v1) { * }

our sub gaurded_rd_kafka_brokers_add(Pointer $kafka, Str $brokers = "")
{
    die PKafka::X::DidNotSpecifyBrokers.new if $brokers.chars == 0;
    my $added = rd_kafka_brokers_add($kafka, $brokers);
    die PKafka::X::DidNotProvideValidBrokers.new(:$brokers) if $added == 0;
}

our $RD_KAFKA_OFFSET_BEGINNING = -2;
our $RD_KAFKA_OFFSET_END = -1;
our $RD_KAFKA_OFFSET_STORED = -1000;
our $RD_KAFKA_OFFSET_TAIL_BASE = -2000;

enum partition_unasigned(
    RD_KAFKA_PARTITION_UA => -1,
);

enum msg_options(
    RD_KAFKA_MSG_F_FREE => 0x1;
    RD_KAFKA_MSG_F_COPY => 0x2;
);

#int rd_kafka_produce (rd_kafka_topic_t *rkt, int32_t partitition, int msgflags, void *payload, size_t len, const void *key, size_t keylen, void *msg_opaque); 
#
our sub rd_kafka_produce(Pointer, int32, int32, Blob, uint64, Str, uint64, Pointer) returns int32 is native('rdkafka', v1) { * }
