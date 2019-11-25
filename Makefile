
TARGET=tests
PKGS=zlib protobuf

CFLAGS=-O3 -Wall
CFLAGS+=$(shell pkg-config --cflags $(PKGS))
LDFLAGS+=$(shell pkg-config --libs $(PKGS))

all: $(TARGET) messages.descr

messages.descr: messages.proto
	protoc --include_imports --descriptor_set_out=$@ $<

messages.pb.cc messages.pb.h: messages.proto
	protoc --cpp_out=. $<

$(TARGET): tests.o messages.pb.o
	g++ -o $@ $^ $(LDFLAGS)

%.o: %.cc pbzfile.h messages.pb.h
	g++ -c -g $< -o $@ $(CFLAGS)

clean:
	rm -f $(TARGET) tests.o messages.pb.cc messages.pb.h messages.pb.o messages.descr

format:
	clang-format -i tests.cc pbzfile.h messages.proto
