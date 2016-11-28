# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Provides facilities for "Record-IO" encoding of data.
"Record-IO" encoding allows one to encode a sequence
of variable-length records by prefixing each record
with its size in bytes:

5\n
hello
6\n
world!

Note that this currently only supports record lengths
encoded as base 10 integer values with newlines as a
delimiter. This is to provide better language portability
portability: parsing a base 10 integer is simple. Most
other "Record-IO" implementations use a fixed-size header
of 4 bytes to directly encode an unsigned 32 bit length.
"""

class Encoder(object):
    def __init__(self, serialize):
        self.serialize = serialize

    def encode(self, record):
        s = self.serialize(record)
        return str(len(s)) + "\n" + s

class Decoder(object):
    HEADER = 0
    RECORD = 1
    FAILED = 2

    def __init__(self, deserialize):
        self.deserialize = deserialize
        self.state = self.HEADER
        self.buffer = ""
        self.length = 0

    def decode(self, data):
        if self.state == self.FAILED:
            raise Exception("Decoder is in a FAILED state")

        records = []

        for c in data:
            if self.state == self.HEADER:
                if c != '\n':
                    self.buffer += c
                    continue
                try:
                    self.length = int(self.buffer)
                except Exception as exception:
                    self.state = self.FAILED;
                    raise Exception("Failed to decode length '{buffer}': {error}"
                                    .format(buffer=self.buffer, error=exception))

                self.buffer = ""
                self.state = self.RECORD;

                # Note that for 0 length records, we immediately decode.
                if self.length <= 0:
                    records.append(self.deserialize(self.buffer));
                    self.state = self.HEADER;

            elif self.state == self.RECORD:
                assert self.length
                assert len(self.buffer) < self.length

                self.buffer += c;

                if len(self.buffer) == self.length:
                  records.append(self.deserialize(self.buffer));
                  self.buffer = ""
                  self.state = self.HEADER;

        return records
