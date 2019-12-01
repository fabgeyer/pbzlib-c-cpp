# Library for serializing protobuf objects - C/C++ version

This library is used for simplifying the serialization and deserialization of [protocol buffer](https://developers.google.com/protocol-buffers/) objects to/from files.
The main use-case is to save and read a large collection of objects of the same type.
Each file contains a header with the description of the protocol buffer, meaning that no compilation of `.proto` description file is required before reading a `pbz` file.

**WARNING:** This library is currently in alpha state and can only parse `pbz` files.


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
