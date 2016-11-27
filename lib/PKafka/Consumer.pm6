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
use PKafka::Kafka;
use PKafka::Config;
use PKafka::Message;

class PKafka::X::Consuming is Exception {
   has $.topic;
   has $.partition;
   has $.offset;
   has $.errstr is rw;
};

class PKafka::X::ConsumeCalledTwice is PKafka::X::Consuming 
{
    method message {"Called consume twice for topic $.topic and partition $.partition"}
};

class PKafka::X::StartingConsumer is PKafka::X::Consuming 
{
    submethod BUILD { self.errstr = PKafka::errno2str() }
    method message {"Error starting to consume partition $.partition of topic $.topic at offset $.offset: $.errstr"}
}

class PKafka::X::StoppingConsumer is PKafka::X::Consuming 
{
    submethod BUILD { self.errstr = PKafka::errno2str() }
    method message {"Error stopping consuming partition $.partition of topic $.topic: $.errstr"}
}

class PKafka::X::StoringOffset is PKafka::X::Consuming 
{
    has $.error-code;
    submethod BUILD(:$!error-code) { self.errstr = PKafka::rd_kafka_err2str($!error-code)}
    method message {"Error storing offset $.offset for partition $.partition of topic $.topic: $.errstr"}
}

class PKafka::X::StoringOffsetForWrongTopic is PKafka::X::Consuming
{
    has $.actual-topic;
    method message {"Error storing offset $.offset for partition $.partition of topic $.topic: Got message from topic $.actual-topic. Make sure to call save-offset with message from correct topic."}
}

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
        die PKafka::X::ConsumeCalledTwice(topic=>self.topic, :$partition) if %!running{$partition};

        my $res = PKafka::rd_kafka_consume_start($!topic, $partition, $offset);
        die PKafka::X::StartingConsumer.new(topic=>self.topic, :$partition, :$offset) if $res == -1;
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
        die PKafka::X::StoppingConsumer.new(topic=>self.topic, :$partition) if $res == -1;
        %!running{$partition} = False;
    }

    method stop-all
    {
        for %!running.kv -> $k, $v { self.stop($k) }
    }

    method messages { $!messages }

    multi method save-offset(Int :$partition, Int :$offset) 
    {
        my $res = PKafka::rd_kafka_offset_store($!topic, $partition, $offset);

        if $res != PKafka::RD_KAFKA_RESP_ERR_NO_ERROR 
        {
            die PKafka::X::StoringOffset.new(
                :$partition, 
                :$offset, 
                topic=>self.topic, 
                err=>$res); 
        }
    }

    multi method save-offset(PKafka::Message $m)
    {
        if $m.topic ne self.topic 
        {
            die PKafka::X::StoringOffsetForWrongTopic.new(
                partition=>$m.partition, 
                offset=>$m.offset, 
                topic=>self.topic,
                actual-topic=>$m.topic);
        }

        self.save-offset(partition=>$m.partition, offset=>$m.offset);
    }

    submethod DESTROY 
    { 
        self.stop-all;
        PKafka::rd_kafka_topic_destroy($!topic);
    }
}
