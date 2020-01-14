# go-car-util

Utility functions commands for working with CAR files, similar to commands found in https://github.com/ipfs/go-car/blob/master/car/main.go but using minimal reads and filesystem `seek()` operations to skip over block data where it's not needed. These utilities are intended to assist with the development of the [CAR spec](https://github.com/ipld/specs/pull/230).

Currently available:

* `car-util header foo.car` - Prints the header of the CAR as single-line JSON
* `car-util index foo.car` - Prints an index for all blocks in the CAR file as line-delimited JSON objects. Each entry of the index contains `cid`, `offset`, `length`, `blockOffset`, `blockLength`. The `blockOffset` and `blockLength` can be used to seek to, and read just the binary block data for the entry.

In addition, `ParseCarHeader(file string) (CarHeader, error)` and `GenerateCarIndex(file string, cb func(BlockEntry) error) (error)` are exported to programatically perform these actions without printing JSON form of this output to stdout.

## License and Copyright

Copyright 2019 Rod Vagg

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
