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

sub validate_set($r, $errstr, $name, $value)
{
    given $r
    {
        when PKafka::RD_KAFKA_CONF_UNKNOWN { die "$name config option is unknown: $errstr";}
        when PKafka::RD_KAFKA_CONF_INVALID { die "The value $value for config option $name is invalid: $errstr";}
    }
}

class PKafka::Config
{ 
    has Pointer $!conf;

    method handle { $!conf;}

    multi method new { my %props; return self.bless(:%props) }
    multi method new(%props) { return self.bless(:%props) }

    submethod BUILD(:%props) 
    {
        $!conf = PKafka::rd_kafka_conf_new();
        for %props.kv -> $k, $v { self!set($k, $v); }
    }

    submethod DESTROY 
    {
        PKafka::rd_kafka_conf_destroy($!conf);
    }

    method !set(Str $name, Str $value) 
    {
        my ($r, $errstr) = PKafka::rd_kafka_conf_set($!conf, $name, $value);
        validate_set($r, $errstr, $name, $value);
    }

}

class PKafka::TopicConfig
{ 
    has Pointer $!conf;

    method handle { $!conf;}

    multi method new { my %props; return self.bless(:%props) }
    multi method new(%props) { return self.bless(:%props) }

    submethod BUILD(:%props) 
    {
        $!conf = PKafka::rd_kafka_topic_conf_new();
        for %props.kv -> $k, $v { self!set($k, $v); }
    }

    submethod DESTROY 
    {
        PKafka::rd_kafka_topic_conf_destroy($!conf);
    }

    method !set(Str $name, Str $value) 
    {
        my ($r, $errstr) = PKafka::rd_kafka_topic_conf_set($!conf, $name, $value);
        validate_set($r, $errstr, $name, $value);
    }
}

