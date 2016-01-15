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
