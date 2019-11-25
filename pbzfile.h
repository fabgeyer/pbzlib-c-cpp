#ifndef PBZFILE_HEADER
#define PBZFILE_HEADER

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#ifdef __cplusplus
#include <google/protobuf/message.h>
#endif

#define CHUNK 16384
#define WINDOW_BIT 15
#define GZIP_ENCODING 16

#define MAGIC "\x41\x42"
#define T_FILE_DESCRIPTOR 1
#define T_DESCRIPTOR_NAME 2
#define T_MESSAGE 3

// Taken from protobuf-c/protobuf-c.c
/**
 * Pack an unsigned 32-bit integer in base-128 varint encoding and return the
 * number of bytes written, which must be 5 or less.
 *
 * \param value
 *      Value to encode.
 * \param[out] out
 *      Packed value.
 * \return
 *      Number of bytes written to `out`.
 */
static inline size_t uint32_pack(uint32_t value, uint8_t *out) {
  unsigned rv = 0;

  if (value >= 0x80) {
    out[rv++] = value | 0x80;
    value >>= 7;
    if (value >= 0x80) {
      out[rv++] = value | 0x80;
      value >>= 7;
      if (value >= 0x80) {
        out[rv++] = value | 0x80;
        value >>= 7;
        if (value >= 0x80) {
          out[rv++] = value | 0x80;
          value >>= 7;
        }
      }
    }
  }
  /* assert: value<128 */
  out[rv++] = value;
  return rv;
}

typedef struct pbzfile_t {
  z_stream zstrm;
  FILE *fpout;
  uint8_t *buf_in;
  uint8_t *buf_out;
#ifdef __cplusplus
  const ::google::protobuf::Descriptor *last_descriptor;
#else
  const ProtobufCMessageDescriptor *last_descriptor;
#endif
} pbzfile;

int pbzfile_init(pbzfile *pbz, const char *filename) {
  int ret;
  pbz->fpout = fopen(filename, "w");
  if (pbz->fpout == NULL) {
    return EXIT_FAILURE;
  }

  pbz->zstrm.data_type = Z_BINARY;
  pbz->zstrm.zalloc = Z_NULL;
  pbz->zstrm.zfree = Z_NULL;
  pbz->zstrm.opaque = Z_NULL;
  ret = deflateInit2(&pbz->zstrm, Z_BEST_COMPRESSION, Z_DEFLATED,
                     WINDOW_BIT | GZIP_ENCODING, 8, Z_DEFAULT_STRATEGY);
  if (ret != Z_OK) {
    return ret;
  }

  pbz->buf_out = (uint8_t *)malloc(CHUNK);
  pbz->last_descriptor = NULL;

  // Write magic header to file
  pbz->buf_in = (uint8_t *)malloc(2);
  pbz->buf_in[0] = MAGIC[0];
  pbz->buf_in[1] = MAGIC[1];

  pbz->zstrm.next_in = pbz->buf_in;
  pbz->zstrm.next_out = pbz->buf_out;
  pbz->zstrm.avail_in = sizeof(char) * 2;
  pbz->zstrm.avail_out = CHUNK;

  ret = deflate(&pbz->zstrm, Z_NO_FLUSH);
  if (ret < 0) {
    return ret;
  }
  size_t have = CHUNK - pbz->zstrm.avail_out;
  fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
  free(pbz->buf_in);

  return Z_OK;
}

int write_descriptor_file(pbzfile *pbz, FILE *fpdescr) {
  // Get file size
  fseek(fpdescr, 0L, SEEK_END);
  int32_t sz = ftell(fpdescr);

  // Write type and length
  pbz->buf_in = (uint8_t *)malloc(5);
  pbz->buf_in[0] = T_FILE_DESCRIPTOR;
  pbz->zstrm.next_in = pbz->buf_in;
  pbz->zstrm.next_out = pbz->buf_out;
  pbz->zstrm.avail_in = uint32_pack(sz, pbz->buf_in + 1);
  pbz->zstrm.avail_in += sizeof(char);
  pbz->zstrm.avail_out = CHUNK;
  int ret = deflate(&pbz->zstrm, Z_NO_FLUSH);
  if (ret < 0) {
    return ret;
  }
  int have = CHUNK - pbz->zstrm.avail_out;
  fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
  free(pbz->buf_in);

  // Read file, compress it and write it to output
  pbz->buf_in = (uint8_t *)malloc(CHUNK);
  fseek(fpdescr, 0L, SEEK_SET);
  while (1) {
    pbz->zstrm.next_in = pbz->buf_in;
    pbz->zstrm.next_out = pbz->buf_out;
    sz = fread(pbz->buf_in, sizeof(char), CHUNK, fpdescr);
    if (sz == 0) {
      break;
    }
    pbz->zstrm.avail_in = sz;
    pbz->zstrm.avail_out = CHUNK;
    ret = deflate(&pbz->zstrm, Z_NO_FLUSH);
    if (ret < 0) {
      return ret;
    }
    have = CHUNK - pbz->zstrm.avail_out;
    fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
  }
  free(pbz->buf_in);

  return Z_OK;
}

int write_descriptor(pbzfile *pbz, const char *filename) {
  FILE *fpdescr = fopen(filename, "r");
  if (fpdescr == NULL) {
    return EXIT_FAILURE;
  }
  int ret = write_descriptor_file(pbz, fpdescr);
  if (ret != Z_OK) {
    return ret;
  }
  fclose(fpdescr);
  return Z_OK;
}

#ifdef __cplusplus
int write_message(pbzfile *pbz, ::google::protobuf::Message *msg) {
#else
int write_message(pbzfile *pbz, const ProtobufCMessage *msg) {
#endif
  int sz, ret, have;

  // Write descriptor name if needed
#ifdef __cplusplus
  if (msg->GetDescriptor() != pbz->last_descriptor) {
    sz = msg->GetDescriptor()->name().length();
#else
  if (msg->descriptor != pbz->last_descriptor) {
    sz = strlen(msg->descriptor->name);
#endif

    pbz->buf_in = (uint8_t *)malloc(5 + sz);
    pbz->buf_in[0] = T_DESCRIPTOR_NAME;
    pbz->zstrm.next_in = pbz->buf_in;
    pbz->zstrm.next_out = pbz->buf_out;
    pbz->zstrm.avail_in = sizeof(char);
    pbz->zstrm.avail_in += uint32_pack(sz, pbz->buf_in + 1);

#ifdef __cplusplus
    memcpy(pbz->buf_in + pbz->zstrm.avail_in,
           msg->GetDescriptor()->name().c_str(), sz);
#else
    memcpy(pbz->buf_in + pbz->zstrm.avail_in, msg->descriptor->name, sz);
#endif
    pbz->zstrm.avail_in += sz;
    pbz->zstrm.avail_out = CHUNK;

    ret = deflate(&pbz->zstrm, Z_PARTIAL_FLUSH);
    if (ret < 0) {
      return ret;
    }
    have = CHUNK - pbz->zstrm.avail_out;
    fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
    free(pbz->buf_in);
#ifdef __cplusplus
    pbz->last_descriptor = msg->GetDescriptor();
#else
    pbz->last_descriptor = msg->descriptor;
#endif
  }

  // Pack message and write to file
#ifdef __cplusplus
  sz = msg->ByteSize();
#else
  sz = protobuf_c_message_get_packed_size(msg);
#endif
  pbz->buf_in = (uint8_t *)malloc(5 + sz);
  pbz->buf_in[0] = T_MESSAGE;
  pbz->zstrm.next_in = pbz->buf_in;
  pbz->zstrm.next_out = pbz->buf_out;
  pbz->zstrm.avail_in = sizeof(char);
  pbz->zstrm.avail_in += uint32_pack(sz, pbz->buf_in + 1);
  if (sz > 0) {
#ifdef __cplusplus
    msg->SerializeToArray(pbz->buf_in + pbz->zstrm.avail_in, sz);
#else
    protobuf_c_message_pack(msg, pbz->buf_in + pbz->zstrm.avail_in);
#endif
    pbz->zstrm.avail_in += sz;
  }
  pbz->zstrm.avail_out = CHUNK;

  ret = deflate(&pbz->zstrm, Z_PARTIAL_FLUSH);
  if (ret < 0) {
    return ret;
  }
  have = CHUNK - pbz->zstrm.avail_out;
  fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
  free(pbz->buf_in);

  return Z_OK;
}

int pbzfile_close(pbzfile *pbz) {
  deflateEnd(&pbz->zstrm);
  free(pbz->buf_out);
  fclose(pbz->fpout);
  return Z_OK;
}

#endif
