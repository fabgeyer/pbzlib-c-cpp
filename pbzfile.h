#pragma once
#ifndef PBZFILE_HEADER
#define PBZFILE_HEADER

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#ifdef __cplusplus
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>
#endif

#define CHUNK (1 << 18)
#define WINDOW_BIT 15
#define GZIP_ENCODING 16

#define MAGIC "\x41\x42"
#define T_FILE_DESCRIPTOR 1
#define T_DESCRIPTOR_NAME 2
#define T_MESSAGE 3
#define T_PROTOBUF_VERSION 4

#define MODE_CLOSED 0
#define MODE_READ 1
#define MODE_WRITE 2

// =============================================================================
// Taken from protobuf-c/protobuf-c.c

static unsigned scan_varint(unsigned len, const uint8_t *data) {
  unsigned i;
  if (len > 10)
    len = 10;
  for (i = 0; i < len; i++)
    if ((data[i] & 0x80) == 0)
      break;
  if (i == len)
    return 0;
  return i + 1;
}

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

static inline uint32_t parse_uint32(unsigned len, const uint8_t *data) {
  uint32_t rv = data[0] & 0x7f;
  if (len > 1) {
    rv |= ((uint32_t)(data[1] & 0x7f) << 7);
    if (len > 2) {
      rv |= ((uint32_t)(data[2] & 0x7f) << 14);
      if (len > 3) {
        rv |= ((uint32_t)(data[3] & 0x7f) << 21);
        if (len > 4)
          rv |= ((uint32_t)(data[4]) << 28);
      }
    }
  }
  return rv;
}

static uint64_t parse_uint64(unsigned len, const uint8_t *data) {
  unsigned shift, i;
  uint64_t rv;

  if (len < 5)
    return parse_uint32(len, data);
  rv = ((uint64_t)(data[0] & 0x7f)) | ((uint64_t)(data[1] & 0x7f) << 7) |
       ((uint64_t)(data[2] & 0x7f) << 14) | ((uint64_t)(data[3] & 0x7f) << 21);
  shift = 28;
  for (i = 4; i < len; i++) {
    rv |= (((uint64_t)(data[i] & 0x7f)) << shift);
    shift += 7;
  }
  return rv;
}

/**
 * Pack a 64-bit unsigned integer using base-128 varint encoding and return the
 * number of bytes written.
 *
 * \param value
 *      Value to encode.
 * \param[out] out
 *      Packed value.
 * \return
 *      Number of bytes written to `out`.
 */
static size_t uint64_pack(uint64_t value, uint8_t *out) {
  uint32_t hi = (uint32_t)(value >> 32);
  uint32_t lo = (uint32_t)value;
  unsigned rv;

  if (hi == 0)
    return uint32_pack((uint32_t)lo, out);
  out[0] = (lo) | 0x80;
  out[1] = (lo >> 7) | 0x80;
  out[2] = (lo >> 14) | 0x80;
  out[3] = (lo >> 21) | 0x80;
  if (hi < 8) {
    out[4] = (hi << 4) | (lo >> 28);
    return 5;
  } else {
    out[4] = ((hi & 7) << 4) | (lo >> 28) | 0x80;
    hi >>= 3;
  }
  rv = 5;
  while (hi >= 128) {
    out[rv++] = hi | 0x80;
    hi >>= 7;
  }
  out[rv++] = hi;
  return rv;
}

// =============================================================================

typedef struct pbzfile_t {
  int _mode;
  z_stream zstrm;
  FILE *fpout;
  FILE *fpin;
  uint8_t *buf_in;
  uint8_t *buf_out;
  uint8_t buf_tl[11];
  int buf_tl_start = 0;
#ifdef __cplusplus
  const ::google::protobuf::Descriptor *last_descriptor;
  google::protobuf::DescriptorPool pool;
  std::map<std::string, const google::protobuf::Descriptor *> descriptorsByName;
  google::protobuf::DynamicMessageFactory dmf;
#else
  const ProtobufCMessageDescriptor *last_descriptor;
#endif
} pbzfile;

int pbzfile_close(pbzfile *pbz);

// =============================================================================
// Write operations

int pbzfile_init(pbzfile *pbz, const char *filename) {
  if (pbz->_mode != MODE_CLOSED) {
    fprintf(stderr, "PBZ file not ready");
    return EXIT_FAILURE;
  }

  int ret;
  pbz->fpin = NULL;
  pbz->buf_in = NULL;
  pbz->buf_out = NULL;
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
  if (ret != Z_OK) {
    return ret;
  }
  size_t have = CHUNK - pbz->zstrm.avail_out;
  fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
  free(pbz->buf_in);

  pbz->_mode = MODE_WRITE;
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
  pbz->zstrm.avail_in = uint64_pack(sz, pbz->buf_in + 1);
  pbz->zstrm.avail_in += sizeof(char);
  pbz->zstrm.avail_out = CHUNK;
  int ret = deflate(&pbz->zstrm, Z_NO_FLUSH);
  if (ret != Z_OK) {
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
    ret = deflate(&pbz->zstrm, Z_FULL_FLUSH);
    if (ret != Z_OK) {
      return ret;
    }
    have = CHUNK - pbz->zstrm.avail_out;
    fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
  }
  free(pbz->buf_in);

  return Z_OK;
}

int write_descriptor(pbzfile *pbz, const char *filename) {
  if (pbz->_mode != MODE_WRITE) {
    fprintf(stderr, "PBZ file not in write mode");
    return EXIT_FAILURE;
  }

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
  if (pbz->_mode != MODE_WRITE) {
    fprintf(stderr, "PBZ file not in write mode");
    return EXIT_FAILURE;
  }

  // Write descriptor name if needed
#ifdef __cplusplus
  if (msg->GetDescriptor() != pbz->last_descriptor) {
    sz = msg->GetDescriptor()->full_name().length();
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
           msg->GetDescriptor()->full_name().c_str(), sz);
#else
    memcpy(pbz->buf_in + pbz->zstrm.avail_in, msg->descriptor->name, sz);
#endif
    pbz->zstrm.avail_in += sz;
    pbz->zstrm.avail_out = CHUNK;

    ret = deflate(&pbz->zstrm, Z_NO_FLUSH);
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

  ret = deflate(&pbz->zstrm, Z_FULL_FLUSH);
  if (ret < 0) {
    return ret;
  }
  have = CHUNK - pbz->zstrm.avail_out;
  fwrite((char *)pbz->buf_out, sizeof(char), have, pbz->fpout);
  free(pbz->buf_in);

  return Z_OK;
}

// =============================================================================
// Read operations

int pbzfile_read(pbzfile *pbz, char *filename) {
  if (pbz->_mode != MODE_CLOSED) {
    fprintf(stderr, "PBZ file not ready");
    return EXIT_FAILURE;
  }

  int ret;
  pbz->_mode = MODE_READ;
  pbz->fpout = NULL;
  pbz->buf_in = NULL;
  pbz->buf_out = NULL;
  pbz->fpin = fopen(filename, "r");
  if (pbz->fpin == NULL) {
    return EXIT_FAILURE;
  }

  pbz->zstrm.data_type = Z_BINARY;
  pbz->zstrm.zalloc = Z_NULL;
  pbz->zstrm.zfree = Z_NULL;
  pbz->zstrm.opaque = Z_NULL;

  ret = inflateInit2(&pbz->zstrm, WINDOW_BIT | GZIP_ENCODING);
  if (ret != Z_OK) {
    return ret;
  }

  pbz->buf_in = (uint8_t *)malloc(CHUNK);
  pbz->zstrm.next_in = pbz->buf_in;
  pbz->zstrm.avail_in = fread(pbz->buf_in, sizeof(char), CHUNK, pbz->fpin);

  // First check the magic header to be sure that we are reading a PBZ file
  uint8_t buf_magic[2];
  pbz->zstrm.next_out = buf_magic;
  pbz->zstrm.avail_out = 2;

  ret = inflate(&pbz->zstrm, Z_NO_FLUSH);
  if (ret != Z_OK) {
    return ret;
  }
  if (memcmp(MAGIC, buf_magic, 2) != 0) {
    fprintf(stderr, "ERR: Invalid magic header\n");
    ret = -1;
  }
  return ret;
}

google::protobuf::Message *next_message(pbzfile *pbz) {
  if (pbz->_mode != MODE_READ) {
    fprintf(stderr, "PBZ file not in read mode");
    return NULL;
  }

  int ret = Z_OK;
  uint8_t *buf_msg = NULL;
  google::protobuf::Message *msg = NULL;

  for (;;) {
    if ((ret == Z_STREAM_END) ||
        (pbz->zstrm.avail_in + pbz->buf_tl_start == 0)) {
      // No more data to be read
      return NULL;
    }

    // Parse type and length first
    memset(pbz->buf_tl + pbz->buf_tl_start, 0,
           sizeof(pbz->buf_tl) - pbz->buf_tl_start);
    pbz->zstrm.next_out = pbz->buf_tl + pbz->buf_tl_start;
    pbz->zstrm.avail_out = sizeof(pbz->buf_tl) - pbz->buf_tl_start;

    for (;;) {
      if (pbz->zstrm.avail_in == 0) {
        // End of file
        break;
      }
      ret = inflate(&pbz->zstrm, Z_NO_FLUSH);
      if (ret == Z_STREAM_END) {
        return NULL;
      } else if (ret != Z_OK) {
        fprintf(stderr, "zlib inflate error: %d in=%d out=%d (L%d)\n", ret,
                pbz->zstrm.avail_in, pbz->zstrm.avail_out, __LINE__);
        return NULL;
      }

      if (pbz->zstrm.avail_out == 0) {
        break;
      } else {
        // Read more data
        pbz->zstrm.next_in = pbz->buf_in;
        pbz->zstrm.avail_in =
            fread(pbz->buf_in, sizeof(char), CHUNK, pbz->fpin);
      }
    }

    uint8_t msg_type = pbz->buf_tl[0];
    if (msg_type == 0) {
      return NULL;
    }
    unsigned sz_int = scan_varint(10, pbz->buf_tl + 1);
    if (sz_int == 0) {
      fprintf(stderr, "Invalid message size (L%d)\n", __LINE__);
      return NULL;
    }

    uint64_t msg_len = parse_uint64(sz_int, pbz->buf_tl + 1);
    unsigned int totbuf = 1 + sz_int + msg_len;

    // Buffer for holding the serialized protobuf message
    buf_msg = (uint8_t *)malloc(msg_len + 1);
    if (buf_msg == NULL) {
      return NULL;
    }
    buf_msg[msg_len] = 0;

    if (totbuf < sizeof(pbz->buf_tl)) {
      // The message already fits in buf_tl. No need to further read the file.
      memcpy(buf_msg, pbz->buf_tl + 1 + sz_int, msg_len);
      memmove(pbz->buf_tl, pbz->buf_tl + totbuf, sizeof(pbz->buf_tl) - totbuf);
      pbz->buf_tl_start = sizeof(pbz->buf_tl) - totbuf;

    } else {
      pbz->buf_tl_start = 0;
      if (sz_int < 10) {
        memcpy(buf_msg, pbz->buf_tl + 1 + sz_int,
               sizeof(pbz->buf_tl) - 1 - sz_int);
      }
      pbz->zstrm.next_out = buf_msg + 10 - sz_int;
      pbz->zstrm.avail_out = msg_len - 10 + sz_int;

      for (;;) {
        ret = inflate(&pbz->zstrm, Z_NO_FLUSH);
        if (ret != Z_OK) {
          fprintf(stderr, "zlib inflate error: %d (L%d)\n", ret, __LINE__);
          goto cleanup;
        }

        if (pbz->zstrm.avail_out == 0) {
          break;
        } else {
          // Read more data
          pbz->zstrm.next_in = pbz->buf_in;
          pbz->zstrm.avail_in =
              fread(pbz->buf_in, sizeof(char), CHUNK, pbz->fpin);
        }
      }
    }

#ifdef __cplusplus
    if (msg_type == T_FILE_DESCRIPTOR) {
      google::protobuf::FileDescriptorSet fs;
      if (!fs.ParseFromArray(buf_msg, msg_len)) {
        fprintf(stderr, "Error parsing file descriptor (L%d)\n", __LINE__);
      } else {
        for (int i = 0; i < fs.file_size(); i++) {
          google::protobuf::FileDescriptorProto fdp = fs.file(i);
          auto desc = pbz->pool.BuildFile(fdp);
          if (desc == NULL) {
            fprintf(stderr, "Error parsing file descriptor (L%d)", __LINE__);
            goto cleanup;

          } else {
            for (int i = 0; i < desc->message_type_count(); i++) {
              auto descriptor = desc->message_type(i);
              pbz->descriptorsByName[descriptor->full_name()] = descriptor;
            }
          }
        }
      }

    } else if (msg_type == T_DESCRIPTOR_NAME) {
      auto it = pbz->descriptorsByName.find((char *)buf_msg);
      if (it == pbz->descriptorsByName.end()) {
        fprintf(stderr, "Could not find: %s\n", buf_msg);
        pbz->last_descriptor = NULL;
        ret = Z_ERRNO;
        goto cleanup;

      } else {
        pbz->last_descriptor = it->second;
      }

    } else if (msg_type == T_MESSAGE) {
      if (pbz->last_descriptor == NULL) {
        fprintf(stderr, "Invalid last_descriptor\n");
        ret = Z_ERRNO;
        goto cleanup;
      }

      msg = pbz->dmf.GetPrototype(pbz->last_descriptor)->New();
      if (!msg->ParseFromArray(buf_msg, msg_len)) {
        fprintf(stderr, "Error parsing message");
        ret = Z_ERRNO;
        goto cleanup;

      } else {
        ret = Z_OK;
        goto cleanup;
      }

    } else if (msg_type == T_PROTOBUF_VERSION) {
      // Do nothing

    } else {
      fprintf(stderr, "Unknown message type: %d\n", msg_type);
      ret = Z_ERRNO;
      goto cleanup;
    }
#endif

    free(buf_msg);
    buf_msg = NULL;
  }

cleanup:
  if (buf_msg != NULL) {
    free(buf_msg);
  }

  if (ret == Z_OK) {
    return msg;
  }
  return NULL;
}

// =============================================================================
// Common read/write operations

int pbzfile_close(pbzfile *pbz) {
  int ret = Z_OK;
  if (pbz->_mode == MODE_READ) {
    if (pbz->fpin != NULL) {
      ret = inflateEnd(&pbz->zstrm);
      fclose(pbz->fpin);
    }

  } else if (pbz->_mode == MODE_WRITE) {
    if (pbz->fpout != NULL) {
      ret = deflate(&pbz->zstrm, Z_FINISH);
      if (ret != Z_STREAM_END) {
        fprintf(stderr, "zlib deflate error: %d\n", ret);
      }
      ret = deflateEnd(&pbz->zstrm) != Z_OK;
      if (ret != Z_OK) {
        fprintf(stderr, "zlib deflateEnd error: %d\n", ret);
      }
      fclose(pbz->fpout);
    }
  }

  if (pbz->buf_out != NULL) {
    free(pbz->buf_out);
  }
  /*
  if (pbz->buf_in != NULL) {
    free(pbz->buf_in);
  }
  */
  pbz->_mode = MODE_CLOSED;
  return ret;
}

#endif
