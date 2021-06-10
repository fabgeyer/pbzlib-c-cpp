# Library for serializing protobuf objects - C/C++ version

This library is used for simplifying the serialization and deserialization of [protocol buffer](https://developers.google.com/protocol-buffers/) objects to/from files.
The main use-case is to save and read a large collection of objects of the same type.
Each file contains a header with the description of the protocol buffer, meaning that no compilation of `.proto` description file is required before reading a `pbz` file.

**WARNING:** This library is currently in alpha state. Read/write supported in C++. Read-only for the C version.

## Usage

Just include the `pbzfile.h` header in your code:
```
#include "pbzfile.h"
```

Examples of reading and writing files are available in `tests.cc`. To compile it use the `Makefile`.

## Installation

For the C version:
```
# apt install libprotobuf-c-dev
```

For the C++ version:
```
# apt install libprotobuf-dev
```


## Versions in other languages

- [Python version](https://github.com/fabgeyer/pbzlib-py)
- [Go version](https://github.com/fabgeyer/pbzlib-go)
- [Java version](https://github.com/fabgeyer/pbzlib-java)
