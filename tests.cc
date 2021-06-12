
#include "messages.pb.h"
#include "pbzfile.h"

int example_write() {
  pbzfile pbz;

  // First open up the file and initialize the pbzfile datastructure
  if (pbzfile_init(&pbz, "out.pbz") != Z_OK) {
    fprintf(stderr, "Error opening file\n");
    return EXIT_FAILURE;
  }

  // Write the descriptor file for the protobuf messages
  if (write_descriptor(&pbz, "messages.descr") != Z_OK) {
    return EXIT_FAILURE;
  }

  // Example of protobuf message which will be written
  test::Header hdr;
  hdr.set_version(1);

  // Write message to the pbz file
  if (write_message(&pbz, &hdr) != Z_OK) {
    return EXIT_FAILURE;
  }

  for (int i = 0; i < 10; i++) {
    // Other example of protobuf message
    test::Object obj;
    obj.set_id(i);

    // Write message to the pbz file
    if (write_message(&pbz, &obj) != Z_OK) {
      return EXIT_FAILURE;
    }
  }

  // Close file
  if (pbzfile_close(&pbz) != Z_OK) {
    fprintf(stderr, "Error closing file\n");
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

#ifdef __cplusplus
int example_read() {
  pbzfile pbz;
  if (pbzfile_read(&pbz, (char *)"out.pbz") != Z_OK) {
    fprintf(stderr, "Error opening file\n");
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

  if (pbzfile_close(&pbz) != Z_OK) {
    fprintf(stderr, "Error closing file\n");
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

int example_read_know_structure() {
  pbzfile pbz;
  test::Header hdr;
  test::Object obj;

  if (pbzfile_read(&pbz, (char *)"out.pbz") != Z_OK) {
    fprintf(stderr, "Error opening file\n");
    return EXIT_FAILURE;
  }

  if (next_message(&pbz, &hdr)) {
    return EXIT_FAILURE;
  }
  std::cout << "version=" << hdr.version() << std::endl;

  for (;;) {
    if (next_message(&pbz, &obj)) {
      break;
    }
    std::cout << "id=" << obj.id() << std::endl;
  }

  if (pbzfile_close(&pbz) != Z_OK) {
    fprintf(stderr, "Error closing file\n");
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
#endif

int main() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  example_write();

#ifdef __cplusplus
  example_read();
  example_read_know_structure();

  google::protobuf::ShutdownProtobufLibrary();
#endif
  return EXIT_SUCCESS;
}
