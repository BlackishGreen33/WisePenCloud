package com.oriole.wisepen.file.storage.service.impl;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oriole.wisepen.file.storage.api.domain.base.StorageRecordBase;
import com.oriole.wisepen.file.storage.api.domain.dto.StorageRecordDTO;
import com.oriole.wisepen.file.storage.api.domain.mq.FileUploadedMessage;
import com.oriole.wisepen.file.storage.api.enums.StorageStatusEnum;
import com.oriole.wisepen.file.storage.config.StorageProperties;
import com.oriole.wisepen.file.storage.domain.entity.StorageRecordEntity;
import com.oriole.wisepen.file.storage.mapper.StorageRecordMapper;
import com.oriole.wisepen.file.storage.mq.KafkaStorageEventPublisher;
import com.oriole.wisepen.file.storage.strategy.StorageManager;
import com.oriole.wisepen.file.storage.strategy.StorageProvider;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class StorageServiceUploadTest {
    @BeforeAll
    static void initializeMybatisMetadata() {
        TableInfoHelper.initTableInfo(
                new MapperBuilderAssistant(new MybatisConfiguration(), "storage-service-test"),
                StorageRecordEntity.class);
    }

    private final List<StorageRecordEntity> records = new ArrayList<>();
    private final List<String> recovered = new ArrayList<>();
    private boolean failMetadata;

    private final StorageProvider provider = (StorageProvider) Proxy.newProxyInstance(
            StorageProvider.class.getClassLoader(), new Class<?>[]{StorageProvider.class}, (object, method, args) -> {
                if (method.getName().equals("getDomain")) return "https://storage.example.invalid";
                if (method.getName().equals("getObjectMetadata")) {
                    if (failMetadata) throw new IllegalStateException("injected metadata failure");
                    return "uploaded".equals(args[0])
                            ? StorageRecordBase.builder().objectKey("uploaded").size(42L).md5("content").build() : null;
                }
                throw new AssertionError("Unexpected storage call: " + method.getName());
            });
    private final StorageRecordMapper mapper = (StorageRecordMapper) Proxy.newProxyInstance(
            StorageRecordMapper.class.getClassLoader(), new Class<?>[]{StorageRecordMapper.class}, (object, method, args) -> {
                return switch (method.getName()) {
                    case "selectList" -> records;
                    case "selectOne" -> records.getFirst();
                    case "updateById" -> 1;
                    case "update" -> 0;
                    default -> throw new AssertionError("Unexpected mapper call: " + method.getName());
                };
            });
    private final StorageServiceImpl service = new StorageServiceImpl(new StorageManager(null) {
        @Override
        public StorageProvider getProvider(Long configId) { return provider; }
    }, mapper, new KafkaStorageEventPublisher(new ObjectMapper()) {
        @Override
        public void publishFileUploadedEvent(FileUploadedMessage message) { recovered.add(message.getObjectKey()); }
    }, new StorageProperties(), new ObjectMapper());

    @Test
    void distinguishesUploadingCompletedDeletedAndMissingRecords() throws Exception {
        for (String key : List.of("uploading", "uploaded", "available", "deleted")) {
            records.add(StorageRecordEntity.builder().objectKey(key).fileId((long) records.size())
                    .status(key.equals("available") ? StorageStatusEnum.AVAILABLE
                            : key.equals("deleted") ? StorageStatusEnum.DELETED : StorageStatusEnum.UPLOADING).build());
        }
        Map<String, StorageRecordDTO> result = service.getUploadRecords(
                        List.of("uploading", "uploaded", "available", "deleted", "missing")).stream()
                .collect(Collectors.toMap(StorageRecordDTO::getObjectKey, Function.identity()));
        assertEquals(StorageStatusEnum.UPLOADING, result.get("uploading").getStatus());
        assertEquals(StorageStatusEnum.AVAILABLE, result.get("uploaded").getStatus());
        assertEquals(42L, result.get("uploaded").getSize());
        assertEquals(StorageStatusEnum.AVAILABLE, result.get("available").getStatus());
        assertEquals(StorageStatusEnum.DELETED, result.get("deleted").getStatus());
        assertFalse(result.containsKey("missing"));
        assertEquals(List.of("uploaded"), recovered);
        ObjectMapper json = new ObjectMapper();
        assertEquals(StorageStatusEnum.UPLOADING, json.readValue(json.writeValueAsString(result.get("uploading")),
                StorageRecordDTO.class).getStatus());
        assertNull(service.getFileRecord("uploading"), "原查单接口仍只返回已完成文件");
    }

    @Test
    void propagatesMetadataFailuresAndAllowsRepeatedDeletion() {
        records.add(StorageRecordEntity.builder().objectKey("uploading").status(StorageStatusEnum.UPLOADING).build());
        failMetadata = true;
        assertThrows(IllegalStateException.class, () -> service.getUploadRecords(List.of("uploading")));
        assertTrue(recovered.isEmpty());
        assertDoesNotThrow(() -> service.deleteFiles(List.of("missing")));
        assertDoesNotThrow(() -> service.deleteFiles(List.of("missing")));
    }
}
