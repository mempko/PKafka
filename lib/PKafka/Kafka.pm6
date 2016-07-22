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

use NativeCall;
use PKafka::Native;
use PKafka::Config;

#This is here instead of PKafka::Native because of a compiler bug.
class rd_kafka_metadata_t is repr('CStruct') {
    has int32   $.broker_cnt;   
    has Pointer $.brokers;   #rd_kafka_metadata_broker
    has int32   $.topic_cnt;
    has Pointer $.topics;   #rd_kafka_metadata_topic
    has int32   $.orig_broker_id;
    has Str $.orig_broker_name; 
};

#rd_kafka_resp_err_t rd_kafka_metadata (rd_kafka_t *rk, int all_topics, rd_kafka_topic_t *only_rkt, const struct rd_kafka_metadata **metadatap, int timeout_ms); 
our sub _rd_kafka_metadata (Pointer, int32, Pointer, Pointer, int32) returns int32 is native('rdkafka', v1) is symbol('rd_kafka_metadata') {*}; 

class PKafka::X::CreatingKafka is Exception
{
    has $.errstr;
    method message {"Error creating kafka object: $.errstr"}
};

class PKafka::Kafka
{ 
    has Pointer $!kafka;

    method handle { $!kafka;}

    method name { PKafka::rd_kafka_name($!kafka);}

    submethod BUILD(PKafka::rd_kafka_type_t :$type, PKafka::Config :$conf)
    {
        my ($pointer, $errstr) = PKafka::rd_kafka_new($type, $conf.handle);
        die PKafka::X::CreatingKafka.new(:$errstr) if $pointer == 0;
        $!kafka = $pointer;
    }

    submethod DESTROY 
    {
        PKafka::rd_kafka_destroy($!kafka);
    }
}
