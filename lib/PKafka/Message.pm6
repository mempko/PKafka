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

#This is here instead of PKafka::Native because of a compiler bug.
class rd_kafka_message_t is repr('CStruct') {
    has int32   $.err;   
    has Pointer $.rkt;
    has int32   $.partition;
    has CArray[uint8] $.payload;
    has uint64  $.len;
    has CArray[uint8] $.key; 
    has uint64  $.key-len; 
    has int64   $.offset;
    has Pointer $._private;
};


#rd_kafka_message_t *rd_kafka_consume (rd_kafka_topic_t *rkt, int32_t partition, int timeout_ms);
our sub rd_kafka_consume(Pointer, int32, int32) returns Pointer[rd_kafka_message_t] is native('rdkafka', v1) { * }

class PKafka::Message
{
    has rd_kafka_message_t $!message;
    has Pointer[rd_kafka_message_t] $!message-ptr;

    submethod BUILD(Pointer[rd_kafka_message_t] :$msg)
    {
        $!message-ptr = $msg;
        $!message = $!message-ptr.deref;
    }

    submethod DESTROY { PKafka::rd_kafka_message_destroy($!message-ptr) }

    method topic returns Str { PKafka::rd_kafka_topic_name($!message.rkt); } 
    method offset returns Int { $!message.offset } 
    method partition returns Int { $!message.partition } 
    method err returns Int { $!message.err } 
    method len returns Int { $!message.len }
    method payload returns Blob { PKafka::array-to-blob($!message.payload, $!message.len) }
    method payload-str returns Str { PKafka::array-to-str-n($!message.payload, $!message.len) }
    method key returns Str { PKafka::array-to-str-n($!message.key, $!message.key-len) }
}

class PKafka::EOF 
{
    has Str $.topic;
    has Int $.partition;
    has Int $.total-consumed;
}

class PKafka::Error 
{
    has Str $.topic;
    has Int $.partition;
    has Str $.what;
    has Int $.err;
}
