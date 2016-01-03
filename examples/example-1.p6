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
