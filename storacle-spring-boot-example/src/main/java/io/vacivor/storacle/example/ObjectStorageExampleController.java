package io.vacivor.storacle.example;

import io.vacivor.storacle.ListObjectsPage;
import io.vacivor.storacle.ObjectMetadata;
import io.vacivor.storacle.ObjectPath;
import io.vacivor.storacle.ObjectWriteResult;
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
import java.util.List;

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
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String prefix,
            @RequestParam MultipartFile file
    ) throws IOException {
        ObjectMetadata metadata = ObjectMetadata.builder()
                .contentType(file.getContentType())
                .contentLength(file.getSize())
                .build();

        UploadRequest request = UploadRequest.builder()
                .bucket(bucket)
                .objectKey(key)
                .originalFilename(file.getOriginalFilename())
                .prefix(prefix)
                .content(file.getInputStream())
                .contentLength(file.getSize())
                .metadata(metadata)
                .build();

        ObjectWriteResult result = storageTemplate.upload(request);
        return new StorageObjectResponse(
                result.path().bucket(),
                result.path().key(),
                result.eTag(),
                result.versionId()
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
}
