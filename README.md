PKafka
======

A Perl 6 client for [Apache Kafka](https://kafka.apache.org/). 
You can use Perl 6's reactive programming features to 'tap' Kafka topics and process messages.

SYNOPSIS
======

    use PKafka::Consumer;
    use PKafka::Message;
    use PKafka::Producer;

    sub MAIN () 
    {
        my $brokers = "127.0.0.1";
        my $test = PKafka::Consumer.new( topic=>"test", brokers=>$brokers);
        my $test2 = PKafka::Consumer.new( topic=>"test2", brokers=>$brokers);
        my $producer = PKafka::Producer.new( topic=>"test2", brokers=>$brokers);

        $test.messages.tap(-> $msg 
        {
            given $msg 
            {
                when PKafka::Message
                {
                    say "got {$msg.offset}: { $msg.payload-str } ";
                    $producer.put("from test '{$msg.payload-str}'");
                }
                when PKafka::EOF
                {
                    say "Messages Consumed { $msg.total-consumed}";
                }
                when PKafka::Error
                {
                    say "Error {$msg.what}";
                    $test.stop;
                }
            }
        });

        $test2.messages.tap(-> $msg 
        {
            given $msg 
            {
                when PKafka::Message
                {
                    say "got {$msg.offset}: { $msg.payload-str } ";
                }
            }
        });

        my $t1 = $test.consume-from-beginning(partition=>0);
        my $t2 = $test2.consume-from-beginning(partition=>0);

        await $t1;
        await $t2;
    }

DEPENDENCIES
======

This library wraps [librdkafka](https://github.com/edenhill/librdkafka) and it requires it to be installed to function.

WARNING
======

This library is ALPHA quality software. Please report any bugs and contribute fixes.

