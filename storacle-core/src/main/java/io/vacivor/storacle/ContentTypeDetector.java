package io.vacivor.storacle;

import java.io.File;
import java.io.InputStream;

public interface ContentTypeDetector {

  String detect(byte[] bytes);

  String detect(InputStream in, String filename);

  String detect(File file);

  default String detect(byte[] bytes, String filename) {
    return detect(bytes);
  }
}
