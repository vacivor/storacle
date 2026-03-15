package io.vacivor.storacle.example;

import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectWriteResult;
import io.vacivor.storacle.ChecksumAlgorithm;
import io.vacivor.storacle.ChecksumMetadata;
import io.vacivor.storacle.StorageObject;
import io.vacivor.storacle.template.ObjectStorageTemplate;
import io.vacivor.storacle.template.UploadRequest;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/objects")
public class ObjectStorageExampleController {
    private final ObjectStorageTemplate storageTemplate;

    public ObjectStorageExampleController(ObjectStorageTemplate storageTemplate) {
        this.storageTemplate = storageTemplate;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public StorageObjectResponse upload(
            @RequestParam String bucket,
            @RequestParam(required = false) String scene,
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String prefix,
            @RequestParam(required = false) List<String> checksumAlgorithm,
            @RequestParam MultipartFile file
    ) throws IOException {
        ObjectMetadata metadata = ObjectMetadata.builder()
                .contentType(file.getContentType())
                .contentLength(file.getSize())
                .build();

        UploadRequest request = UploadRequest.builder()
                .bucket(bucket)
                .scene(scene)
                .objectKey(key)
                .originalFilename(file.getOriginalFilename())
                .prefix(prefix)
                .content(file.getInputStream())
                .contentLength(file.getSize())
                .metadata(metadata)
                .checksumAlgorithms(checksumAlgorithm == null
                        ? List.of()
                        : checksumAlgorithm.stream().map(ChecksumAlgorithm::from).toList())
                .build();

        ObjectWriteResult result = storageTemplate.upload(request);
        return new StorageObjectResponse(
                result.path().bucket(),
                result.path().key(),
                result.originalFilename(),
                result.contentType(),
                result.contentLength(),
                result.eTag(),
                result.versionId(),
                toResponseChecksums(result)
        );
    }

    @GetMapping("/content")
    public ResponseEntity<byte[]> download(
            @RequestParam String bucket,
            @RequestParam String key
    ) throws IOException {
        ObjectPath path = ObjectPath.of(bucket, key);
        try (StorageObject object = storageTemplate.get(path)) {
            byte[] body = object.content().readAllBytes();
            String contentType = object.metadata().contentType() == null
                    ? MediaType.APPLICATION_OCTET_STREAM_VALUE
                    : object.metadata().contentType();

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, ContentDisposition.inline().filename(key).build().toString())
                    .contentType(MediaType.parseMediaType(contentType))
                    .contentLength(body.length)
                    .body(body);
        }
    }

    @GetMapping("/metadata")
    public ObjectMetadataResponse metadata(
            @RequestParam String bucket,
            @RequestParam String key
    ) throws IOException {
        ObjectPath path = ObjectPath.of(bucket, key);
        try (StorageObject object = storageTemplate.get(path)) {
            ObjectMetadata metadata = object.metadata();
            return new ObjectMetadataResponse(
                    bucket,
                    key,
                    metadata.contentType(),
                    metadata.contentLength(),
                    metadata.contentDisposition(),
                    metadata.cacheControl(),
                    metadata.userMetadata(),
                    toResponseChecksums(ChecksumMetadata.extract(metadata.userMetadata()))
            );
        }
    }

    @GetMapping("/exists")
    public boolean exists(
            @RequestParam String bucket,
            @RequestParam String key
    ) {
        return storageTemplate.exists(ObjectPath.of(bucket, key));
    }

    @GetMapping
    public ListObjectsResponse list(
            @RequestParam String bucket,
            @RequestParam(required = false) String prefix,
            @RequestParam(defaultValue = "100") int maxKeys,
            @RequestParam(required = false) String continuationToken
    ) {
        ListObjectsPage page = storageTemplate.list(bucket, prefix, maxKeys, continuationToken);
        List<ObjectSummaryResponse> objects = page.objects().stream()
                .map(object -> new ObjectSummaryResponse(
                        object.path().bucket(),
                        object.path().key(),
                        object.size(),
                        object.lastModified(),
                        object.eTag()
                ))
                .toList();
        return new ListObjectsResponse(objects, page.nextContinuationToken(), page.truncated());
    }

    @DeleteMapping
    public boolean delete(
            @RequestParam String bucket,
            @RequestParam String key
    ) {
        return storageTemplate.delete(ObjectPath.of(bucket, key));
    }

    private static Map<String, String> toResponseChecksums(ObjectWriteResult result) {
        return toResponseChecksums(result.checksums());
    }

    private static Map<String, String> toResponseChecksums(Map<ChecksumAlgorithm, String> checksums) {
        return checksums.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        entry -> entry.getKey().value(),
                        Map.Entry::getValue,
                        (left, right) -> left,
                        LinkedHashMap::new
                ));
    }
}
