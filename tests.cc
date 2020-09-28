
#include "messages.pb.h"
#include "pbzfile.h"

int main() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // -----------------------------------------------------------------
  // Write file

  int ret;
  pbzfile pbz;
  test::Header hdr;
  test::Object obj;

  if (pbzfile_init(&pbz, "out.pbz") != Z_OK) {
    return EXIT_FAILURE;
  }

  if (write_descriptor(&pbz, "messages.descr") != Z_OK) {
    return EXIT_FAILURE;
  }

  hdr.set_version(1);
  if (write_message(&pbz, &hdr) != Z_OK) {
    return EXIT_FAILURE;
  }

  for (int i = 0; i < 10; i++) {
    obj.set_id(i);
    if (write_message(&pbz, &obj) != Z_OK) {
      return EXIT_FAILURE;
    }
  }

  if (pbzfile_close(&pbz) != Z_OK) {
    return EXIT_FAILURE;
  }

  // -----------------------------------------------------------------
  // Read file

  if (pbzfile_read(&pbz, (char *)"out.pbz") != Z_OK) {
    return EXIT_FAILURE;
  }

  for (;;) {
    google::protobuf::Message *msg = next_message(&pbz);
    if (msg == NULL) {
      break;
    }
    std::cout << msg->DebugString() << std::endl;
    delete msg;
  }

  google::protobuf::ShutdownProtobufLibrary();
  return ret;
}
