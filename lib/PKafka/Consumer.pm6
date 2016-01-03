=begin license

Copyright (c) 2016 Maxim Noah Khailo, All Rights Reserved

This file is part of PKafka.

PKafka is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

PKafka is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with PKafka.  If not, see <http://www.gnu.org/licenses/>.
use NativeCall;

=end license

use NativeCall;
use PKafka::Native;
use PKafka::Kafka;
use PKafka::Config;
use PKafka::Message;

class PKafka::Consumer 
{
    has Pointer $!topic;
    has PKafka::Kafka $!kafka;
    has Supply $!messages;
    has %!running{Int};
    has $!message-supplier;
    has $!config;
    has $!topic-config;

    method topic { PKafka::rd_kafka_topic_name($!topic);}

    submethod BUILD(
        Str :$topic!, 
        PKafka::Config :$!config = PKafka::Config.new, 
        PKafka::TopicConfig :$!topic-config = PKafka::TopicConfig.new,
        Str :$brokers!) 
    {
        $!message-supplier = Supplier.new;
        $!messages = $!message-supplier.Supply;

        $!kafka = PKafka::Kafka.new( type=>PKafka::RD_KAFKA_CONSUMER, conf=>$!config);
        PKafka::gaurded_rd_kafka_brokers_add($!kafka.handle, $brokers);
        $!topic = PKafka::rd_kafka_topic_new($!kafka.handle, $topic, $!topic-config.handle);
    }

    method consume-from-beginning(Int :$partition)
    {
        self.consume(partition=>$partition, offset=>$PKafka::RD_KAFKA_OFFSET_BEGINNING);
    }

    method consume-from-end(Int :$partition)
    {
        self.consume(partition=>$partition, offset=>$PKafka::RD_KAFKA_OFFSET_END);
    }

    method consume-from-last(Int :$partition)
    {
        self.consume(partition=>$partition, offset=>$PKafka::RD_KAFKA_OFFSET_STORED);
    }

    method consume(Int :$partition, Int :$offset) 
    {
        die "Called consume twice" if %!running{$partition};

        my $res = PKafka::rd_kafka_consume_start($!topic, $partition, $offset);
        die "Error starting parrition $partition of topic { self.topic } at offset $offset: { PKafka::errno2str() }" if $res == -1;
        %!running{$partition} = True;

        start 
        { 
            my $consumed-messages = 0;
            while %!running{$partition}
            {
                my $timeout-ms = 100;
                my $msg-ptr = PKafka::rd_kafka_consume($!topic, $partition, $timeout-ms);

                next if not $msg-ptr;

                my $msg = PKafka::Message.new(msg=>$msg-ptr);
                given $msg.err
                {
                    when 0
                    { 
                        $consumed-messages++;
                        $!message-supplier.emit($msg); 
                    }
                    when PKafka::RD_KAFKA_RESP_ERR__PARTITION_EOF 
                    { 
                        $!message-supplier.emit(PKafka::EOF.new(
                                topic=>self.topic,
                                partition=>$partition, 
                                total-consumed=>$consumed-messages));
                        next;
                    }
                    default 
                    { 
                        $!message-supplier.emit(PKafka::Error.new(
                                topic=>self.topic,
                                partition=>$partition, 
                                err=>$msg.err, 
                                what=>PKafka::rd_kafka_err2str($msg.err)));
                    }
                }
            }
            $!message-supplier.done;
        }
    }

    method stop(Int $partition) 
    {
        return if %!running{$partition} == False;
        my $res = PKafka::rd_kafka_consume_stop($!topic, $partition);
        die "Error stopping consuming partition $partition of topic { self.topic }: { PKafka::errno2str() }" if $res == -1;
        %!running{$partition} = False;
    }

    method stop-all
    {
        for %!running.kv -> $k, $v { self.stop($k) }
    }

    method messages { $!messages }

    submethod DESTROY 
    { 
        self.stop-all;
        PKafka::rd_kafka_topic_destroy($!topic);
    }
}
