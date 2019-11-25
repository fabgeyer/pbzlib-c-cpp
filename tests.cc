
#include "messages.pb.h"
#include "pbzfile.h"

int main() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  int ret;
  pbzfile pbz;
  ret = pbzfile_init(&pbz, "out.pbz");
  if (ret != Z_OK) {
    return ret;
  }

  ret = write_descriptor(&pbz, "messages.descr");
  if (ret != Z_OK) {
    return ret;
  }

  test::Header hdr;
  hdr.set_version(1);
  ret = write_message(&pbz, &hdr);
  if (ret != Z_OK) {
    return ret;
  }

  test::Object obj;
  for (int i = 0; i < 10; i++) {
    obj.set_id(i);
    ret = write_message(&pbz, &obj);
    if (ret != Z_OK) {
      return ret;
    }
  }

  ret = pbzfile_close(&pbz);
  if (ret != Z_OK) {
    return ret;
  }

  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
