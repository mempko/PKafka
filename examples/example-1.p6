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
