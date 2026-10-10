package com.oriole.wisepen.ai.asset.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import com.oriole.wisepen.ai.asset.domain.base.AssetInfoBase;
import com.oriole.wisepen.ai.asset.domain.dto.req.AssetUploadInitRequest;
import com.oriole.wisepen.ai.asset.domain.dto.req.AssetUploadCancelRequest;
import com.oriole.wisepen.ai.asset.domain.dto.req.AssetDeleteRequest;
import com.oriole.wisepen.ai.asset.domain.entity.SkillEntity;
import com.oriole.wisepen.ai.asset.domain.entity.SkillVersionBundleEntity;
import com.oriole.wisepen.ai.asset.domain.entity.AgentVersionBundleEntity;
import com.oriole.wisepen.ai.asset.domain.base.AgentSpecInfoBase;
import com.oriole.wisepen.ai.asset.enums.AssetResourceType;
import com.oriole.wisepen.ai.asset.enums.AssetUploadStatus;
import com.oriole.wisepen.ai.asset.enums.VersionStatus;
import com.oriole.wisepen.ai.asset.mq.AIAssetEventPublisher;
import com.oriole.wisepen.ai.asset.repository.AIResourceBaseRepository;
import com.oriole.wisepen.ai.asset.repository.VersionBundleBaseRepository;
import com.oriole.wisepen.ai.asset.repository.SkillVersionBundleRepository;
import com.oriole.wisepen.ai.asset.repository.SkillRepository;
import com.oriole.wisepen.ai.asset.repository.AgentVersionBundleRepository;
import com.oriole.wisepen.common.core.domain.R;
import com.oriole.wisepen.common.core.exception.ServiceException;
import com.oriole.wisepen.file.storage.api.domain.mq.FileUploadedMessage;
import com.oriole.wisepen.file.storage.api.enums.StorageSceneEnum;
import com.oriole.wisepen.file.storage.api.enums.StorageStatusEnum;
import com.oriole.wisepen.common.core.constant.CommonError;
import com.oriole.wisepen.file.storage.api.domain.dto.UploadInitRespDTO;
import com.oriole.wisepen.file.storage.api.domain.dto.StorageRecordDTO;
import com.oriole.wisepen.file.storage.api.feign.RemoteStorageService;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import static org.junit.jupiter.api.Assertions.assertThrows;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/** 运行真实 Service，只替换数据库、存储服务和消息发布边界。 */
public class VersionServiceUploadTest {
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    private final Map<Integer, SkillVersionBundleEntity> versions = new HashMap<>();
    private final List<String> deleted = new ArrayList<>();
    private int sequence;
    private boolean flash;
    private int publishedVersion;
    private boolean failSecondInit;
    private boolean failDraftInsert;
    private boolean failVersionUpdate;
    private boolean failStorageRead;
    private boolean failStorageEnvelope;
    private final Set<String> missingObjects = new HashSet<>();
    private final Set<String> deletedObjects = new HashSet<>();
    private final Set<String> storedObjects = new HashSet<>();
    private Runnable concurrentWrite;

    @SuppressWarnings("unchecked")
    private final VersionBundleBaseRepository<SkillVersionBundleEntity> repository = proxy(
            VersionBundleBaseRepository.class, (object, method, args) -> switch (method.getName()) {
                case "findByResourceIdAndVersion" -> Optional.ofNullable(versions.get(args[1])).map(this::copy);
                case "findByResourceId" -> versions.values().stream().map(this::copy).toList();
                case "findFirstByAssetObjectKey" -> versions.values().stream()
                        .filter(value -> allAssets(value).stream().anyMatch(asset -> args[0].equals(asset.getObjectKey())))
                        .findFirst().map(this::copy);
                case "updateDraftAssets", "publishDraft" -> {
                    if (concurrentWrite != null) {
                        Runnable write = concurrentWrite;
                        concurrentWrite = null;
                        write.run();
                    }
                    SkillVersionBundleEntity current = versions.values().stream()
                            .filter(value -> Objects.equals(args[0], value.getId())).findFirst().orElse(null);
                    if (current == null || current.getStatus() != VersionStatus.DRAFT
                            || !Objects.equals(current.getRevision(), args[1])) yield 0L;
                    SkillVersionBundleEntity next = copy(current);
                    if (method.getName().equals("updateDraftAssets")) {
                        next.setAssets((List<AssetInfoBase>) args[2]);
                        next.setPendingAssets((List<AssetInfoBase>) args[3]);
                    } else {
                        next.setStatus(VersionStatus.PUBLISHED);
                    }
                    next.setRevision(current.getRevision() == null ? 1 : current.getRevision() + 1);
                    versions.put(next.getVersion(), copy(next));
                    yield 1L;
                }
                case "deleteByResourceIdIn" -> {
                    versions.clear();
                    yield null;
                }
                case "save", "insert" -> {
                    if (failDraftInsert) throw new IllegalStateException("injected draft creation failure");
                    SkillVersionBundleEntity value = copy((SkillVersionBundleEntity) args[0]);
                    if (method.getName().equals("insert") && versions.containsKey(value.getVersion())) {
                        throw new org.springframework.dao.DuplicateKeyException("duplicate draft");
                    }
                    if (value.getId() == null) value.setId("draft-" + value.getVersion());
                    versions.put(value.getVersion(), value);
                    yield copy(value);
                }
                default -> throw new AssertionError("Unexpected repository call: " + method.getName());
            });
    private final AIResourceBaseRepository<SkillEntity> resources = proxy(AIResourceBaseRepository.class,
            (object, method, args) -> {
                if (method.getName().equals("findByResourceId")) {
                    return Optional.of(SkillEntity.builder().resourceId("skill").version(publishedVersion).build());
                }
                if (method.getName().equals("updateVersionByResourceId")) {
                    if (failVersionUpdate) throw new IllegalStateException("injected version update failure");
                    publishedVersion = Math.max(publishedVersion, (Integer) args[1]);
                    return null;
                }
                throw new AssertionError("Unexpected resource call: " + method.getName());
            });
    private final RemoteStorageService storage = proxy(RemoteStorageService.class, (object, method, args) -> {
        if (method.getName().equals("getUploadRecords")) {
            if (failStorageRead) throw new IllegalStateException("injected storage lookup failure");
            if (failStorageEnvelope) return R.fail(CommonError.INTERNAL_ERROR);
            return R.ok(((List<?>) args[0]).stream().map(String.class::cast)
                    .filter(key -> !missingObjects.contains(key))
                    .map(key -> StorageRecordDTO.builder().objectKey(key).size(8L)
                            .status(deletedObjects.contains(key) ? StorageStatusEnum.DELETED
                                    : storedObjects.contains(key) ? StorageStatusEnum.AVAILABLE : StorageStatusEnum.UPLOADING)
                            .build()).toList());
        }
        if (method.getName().equals("initUpload")) {
            if (failSecondInit && sequence == 1) throw new IllegalStateException("injected storage failure");
            String key = "new-" + ++sequence;
            if (flash) uploaded(key, true);
            return R.ok(UploadInitRespDTO.builder().objectKey(key).flashUploaded(flash).build());
        }
        throw new AssertionError("Unexpected storage call: " + method.getName());
    });
    private final SkillVersionServiceImpl service = new SkillVersionServiceImpl(repository, resources, storage,
            new AIAssetEventPublisher(JSON) {
                @Override
                public void publishFileDeleteEvent(List<String> keys) {
                    deleted.addAll(keys);
                }
            });

    VersionServiceUploadTest() {
        versions.put(1, SkillVersionBundleEntity.builder().id("draft").resourceId("skill").version(1)
                .status(VersionStatus.DRAFT).assets(new ArrayList<>(List.of(AssetInfoBase.builder()
                        .id("existing").name("SKILL.md").path("/").objectKey("old-body")
                        .assetResourceType(AssetResourceType.MD).uploadStatus(AssetUploadStatus.AVAILABLE).size(3L)
                        .build()))).build());
    }

    private SkillVersionBundleEntity copy(SkillVersionBundleEntity value) {
        return JSON.convertValue(value, SkillVersionBundleEntity.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<?> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private List<AssetInfoBase> allAssets(SkillVersionBundleEntity value) {
        List<AssetInfoBase> result = new ArrayList<>(value.getAssets());
        if (value.getPendingAssets() != null) result.addAll(value.getPendingAssets());
        return result;
    }

    private String activeKey() {
        return versions.get(1).getAssets().stream().filter(asset -> asset.getName().equals("SKILL.md"))
                .findFirst().orElseThrow().getObjectKey();
    }

    private void cancel(String key) {
        service.cancelUploadAssets(AssetUploadCancelRequest.builder().resourceId("skill").draftVersion(1)
                .objectKeys(List.of(key)).build());
    }

    private void uploaded(String key, boolean isFlash) {
        service.handleFileUploaded(FileUploadedMessage.builder().scene(StorageSceneEnum.PRIVATE_SKILL_ASSET)
                .objectKey(key).size(8L).flashUploaded(isFlash).build());
    }

    private void init(String... names) {
        service.initUploadAssets(AssetUploadInitRequest.builder().resourceId("skill").draftVersion(1)
                .assets(Arrays.stream(names.length == 0 ? new String[]{"SKILL.md"} : names)
                        .map(name -> AssetUploadInitRequest.AssetUploadRequest.builder().path("/").name(name)
                                .assetResourceType(AssetResourceType.MD).expectedSize(8L).build()).toList()).build());
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void rejects(Runnable action) {
        try {
            action.run();
            throw new AssertionError("操作应被拒绝");
        } catch (ServiceException expected) {
            // 预期业务拒绝。
        }
    }

    @Test
    void preservesFilesAcrossUploadFailuresAndPublication() {
        VersionServiceUploadTest failed = new VersionServiceUploadTest();
        MongoMappingContext context = new MongoMappingContext();
        context.setSimpleTypeHolder(MongoCustomConversions.create(adapter -> {}).getSimpleTypeHolder());
        MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
        converter.afterPropertiesSet();
        Document legacyDocument = new Document();
        converter.write(failed.versions.get(1), legacyDocument);
        check(!legacyDocument.containsKey("revision"), "首次保存不写入 null 修订号，旧文档可通过 $inc 初始化");
        check(converter.read(SkillVersionBundleEntity.class, legacyDocument).getRevision() == null,
                "没有修订号的旧文档正常读取");
        failed.init();
        check("old-body".equals(failed.activeKey()), "申请上传凭证不得替换最后成功的正文");
        check(failed.versions.get(1).getAssets().getFirst().getUploadStatus() == AssetUploadStatus.AVAILABLE,
                "申请上传凭证不得使旧文件不可用");
        check(!failed.deleted.contains("old-body"), "新上传成功前不得回收旧对象");
        rejects(() -> failed.service.publishVersion("skill"));
        failed.cancel("new-1");
        failed.cancel("new-1");
        failed.uploaded("new-1", false);
        check("old-body".equals(failed.activeKey()), "取消及迟到回调不得破坏旧正文");
        check(failed.versions.get(1).getPendingAssets().isEmpty(), "取消后不残留待上传项");
        failed.service.publishVersion("skill");
        check(failed.versions.get(1).getStatus() == VersionStatus.PUBLISHED, "取消后可以发布旧内容");
        check(failed.versions.get(2).getPendingAssets() == null, "新草稿不继承待上传项");
        rejects(() -> failed.cancel("new-1"));

        VersionServiceUploadTest success = new VersionServiceUploadTest();
        success.init();
        success.uploaded("new-1", false);
        success.cancel("new-1");
        success.uploaded("new-1", false);
        check("new-1".equals(success.activeKey()), "成功回调后才能替换，迟到取消不得删除成功文件");
        check(success.deleted.contains("old-body") && !success.deleted.contains("new-1"), "仅回收失去引用的旧对象");
        check("existing".equals(success.versions.get(1).getAssets().getFirst().getId()), "覆盖后保留文件 ID");

        VersionServiceUploadTest retry = new VersionServiceUploadTest();
        retry.init();
        retry.init();
        retry.cancel("new-1");
        retry.uploaded("new-1", false);
        check("old-body".equals(retry.activeKey()), "旧请求的取消和回调不能提交旧候选");
        retry.uploaded("new-2", false);
        check("new-2".equals(retry.activeKey()) && !retry.deleted.contains("new-2"), "重试成功保留新内容");

        VersionServiceUploadTest shared = new VersionServiceUploadTest();
        SkillVersionBundleEntity published = shared.copy(shared.versions.get(1));
        published.setStatus(VersionStatus.PUBLISHED);
        published.setVersion(0);
        published.setId("published");
        shared.versions.put(0, published);
        shared.init();
        shared.uploaded("new-1", false);
        check(!shared.deleted.contains("old-body"), "已发布版本引用的旧对象不得删除");

        VersionServiceUploadTest removed = new VersionServiceUploadTest();
        removed.init();
        removed.service.deleteAssets(AssetDeleteRequest.builder().resourceId("skill").draftVersion(1)
                .assetIds(List.of("existing")).build());
        removed.uploaded("new-1", false);
        check(removed.versions.get(1).getAssets().isEmpty() && removed.versions.get(1).getPendingAssets().isEmpty(),
                "主动删除必须同时移除原文件和候选，迟到回调不能复活文件");

        VersionServiceUploadTest concurrent = new VersionServiceUploadTest();
        concurrent.init("SKILL.md", "notes.md");
        concurrent.concurrentWrite = () -> concurrent.uploaded("new-2", false);
        concurrent.uploaded("new-1", false);
        check(concurrent.versions.get(1).getAssets().size() == 2
                && concurrent.versions.get(1).getPendingAssets().isEmpty(), "并发回调不能丢掉其他文件");
        check("new-1".equals(concurrent.activeKey()), "冲突后重新读取并提交当前候选");

        VersionServiceUploadTest cancelFirst = new VersionServiceUploadTest();
        cancelFirst.init();
        cancelFirst.concurrentWrite = () -> cancelFirst.cancel("new-1");
        cancelFirst.uploaded("new-1", false);
        check("old-body".equals(cancelFirst.activeKey()), "取消先提交时，冲突重试的回调不得复活候选");
        VersionServiceUploadTest callbackFirst = new VersionServiceUploadTest();
        callbackFirst.init();
        callbackFirst.concurrentWrite = () -> callbackFirst.uploaded("new-1", false);
        callbackFirst.cancel("new-1");
        check("new-1".equals(callbackFirst.activeKey()) && !callbackFirst.deleted.contains("new-1"),
                "回调先提交时，冲突重试的取消不得删除成功文件");

        VersionServiceUploadTest publishing = new VersionServiceUploadTest();
        publishing.concurrentWrite = publishing::init;
        rejects(() -> publishing.service.publishVersion("skill"));
        check(publishing.versions.get(1).getStatus() == VersionStatus.DRAFT, "并发初始化上传后不能发布旧快照");

        VersionServiceUploadTest delayed = new VersionServiceUploadTest();
        delayed.init();
        delayed.storedObjects.add("new-1");
        check("new-1".equals(delayed.service.getVersionBundle("skill", 1).getAssets().getFirst().getObjectKey()),
                "PUT 成功而消息尚未消费时，读取草稿必须确认存储并返回新正文");
        delayed.uploaded("new-1", false);
        check(!delayed.deleted.contains("new-1"), "补偿后重复回调不得删除已提交正文");

        VersionServiceUploadTest delayedPublish = new VersionServiceUploadTest();
        delayedPublish.init();
        delayedPublish.storedObjects.add("new-1");
        delayedPublish.service.publishVersion("skill");
        check("new-1".equals(delayedPublish.activeKey()), "立即发布也必须确认尚未消费的成功上传");

        for (boolean failInsert : List.of(true, false)) {
            VersionServiceUploadTest interrupted = new VersionServiceUploadTest();
            interrupted.failDraftInsert = failInsert;
            interrupted.failVersionUpdate = !failInsert;
            try {
                interrupted.service.publishVersion("skill");
                throw new AssertionError("发布应在持久化边界失败");
            } catch (IllegalStateException expected) {
                // 模拟发布中途的数据库失败。
            }
            check(interrupted.publishedVersion == 0, "下一草稿可用前不能推进主版本号");
            interrupted.failDraftInsert = false;
            interrupted.failVersionUpdate = false;
            interrupted.service.publishVersion("skill");
            check(interrupted.publishedVersion == 1 && interrupted.versions.get(2).getStatus() == VersionStatus.DRAFT,
                    "发布中断后重试可继续完成，不会永久卡在已发布草稿");
            interrupted.versions.get(2).getAssets().getFirst().setObjectKey("new-draft-edit");
            interrupted.service.createDraftVersion("skill", 2);
            check("new-draft-edit".equals(interrupted.versions.get(2).getAssets().getFirst().getObjectKey()),
                    "重复创建下一草稿不得覆盖用户已保存的内容");
        }

        VersionServiceUploadTest flash = new VersionServiceUploadTest();
        flash.flash = true;
        flash.init();
        check("new-1".equals(flash.activeKey()) && !flash.deleted.contains("new-1"), "秒传消息早于登记时不得误删");
        check(flash.versions.get(1).getPendingAssets().isEmpty(), "秒传立即提交且不残留候选");

        VersionServiceUploadTest failedInit = new VersionServiceUploadTest();
        failedInit.flash = true;
        failedInit.failSecondInit = true;
        rejects(() -> failedInit.init("SKILL.md", "notes.md"));
        check("old-body".equals(failedInit.activeKey()) && failedInit.deleted.contains("new-1")
                && !failedInit.deleted.contains("old-body"), "批次申请失败只回收未登记候选，不改变旧正文");

        VersionServiceUploadTest legacy = new VersionServiceUploadTest();
        legacy.versions.get(1).getAssets().getFirst().setUploadStatus(AssetUploadStatus.UPLOADING);
        legacy.uploaded("old-body", false);
        check(legacy.versions.get(1).getAssets().getFirst().getUploadStatus() == AssetUploadStatus.AVAILABLE,
                "兼容升级前已登记的上传及缺少 revision 的旧文档");

        VersionServiceUploadTest deletedResource = new VersionServiceUploadTest();
        deletedResource.init();
        deletedResource.service.deleteAllVersionsByResourceIds(List.of("skill"));
        check(deletedResource.deleted.containsAll(List.of("old-body", "new-1")), "删除资源同时回收活动和候选对象");
        System.out.println("PASS: upload preservation, cancellation, retries, callbacks, publication and deletion");
    }

    @Test
    void releasesAbandonedUploadAfterStorageCleanup() {
        init("SKILL.md", "discarded.md");
        rejects(() -> service.publishVersion("skill"));
        check(versions.get(1).getPendingAssets().size() == 2, "仍在上传的候选必须保留");
        missingObjects.add("new-1");
        deletedObjects.add("new-2");
        // 模拟取消请求丢失，随后储存服务已清理失败记录；旧正文仍应允许发布。
        service.publishVersion("skill");
        check(versions.get(1).getPendingAssets().isEmpty(), "已清理的候选不应永久阻止发布");
        check("old-body".equals(activeKey()) && !deleted.contains("old-body"), "清理候选保留旧正文");
    }

    @Test
    void propagatesStorageFailureWithoutChangingFiles() {
        init();
        failStorageRead = true;
        assertThrows(IllegalStateException.class, () -> service.getVersionBundle("skill", 1));
        check("old-body".equals(activeKey()) && versions.get(1).getPendingAssets().size() == 1,
                "存储查询异常不能被当成文件不存在");
    }

    @Test
    void rejectsFailedStorageEnvelopeWithoutCancellingUploads() {
        init();
        failStorageEnvelope = true;
        assertThrows(ServiceException.class, () -> service.getVersionBundle("skill", 1));
        check(versions.get(1).getPendingAssets().size() == 1 && deleted.isEmpty(),
                "失败响应的空 data 不能被当成已清理文件");
    }

    @Test
    void cleanupDoesNotCancelConcurrentRetryOrCompletedUpload() {
        init();
        missingObjects.add("new-1");
        concurrentWrite = this::init;
        var recovered = service.getVersionBundle("skill", 1);
        check(recovered.getPendingAssets().size() == 1
                        && "new-2".equals(recovered.getPendingAssets().getFirst().getObjectKey()),
                "清理旧对象不能取消并发的同名重试");
        check("old-body".equals(activeKey()) && !deleted.contains("new-2"), "重试完成前仍保留旧正文");

        VersionServiceUploadTest completed = new VersionServiceUploadTest();
        completed.init();
        completed.missingObjects.add("new-1");
        completed.concurrentWrite = () -> completed.uploaded("new-1", false);
        completed.service.getVersionBundle("skill", 1);
        check("new-1".equals(completed.activeKey()) && !completed.deleted.contains("new-1"),
                "清理前已经提交的成功文件不能被删除");
    }

    /** 通过系统属性传入 Mongo URI；只创建并清理本次随机命名的隔离数据库。 */
    @Test
    void verifiesMongoRepositories() {
        String uri = System.getProperty("wisepen.test.mongoUri");
        Assumptions.assumeTrue(uri != null && !uri.isBlank(), "未配置隔离 MongoDB");
        try (var client = MongoClients.create(uri)) {
            MongoTemplate mongo = new MongoTemplate(client, "wisepen_upload_check_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                var factory = new MongoRepositoryFactory(mongo);
                var bundles = factory.getRepository(SkillVersionBundleRepository.class);
                var resources = factory.getRepository(SkillRepository.class);
                var agents = factory.getRepository(AgentVersionBundleRepository.class);
                VersionServiceUploadTest boundaries = new VersionServiceUploadTest();
                String resourceId = new ObjectId().toHexString();
                resources.insert(SkillEntity.builder().resourceId(resourceId).version(0).build());
                SkillVersionBundleEntity initial = boundaries.copy(boundaries.versions.get(1));
                initial.setId(null);
                initial.setResourceId(resourceId);
                bundles.insert(initial);
                var real = new SkillVersionServiceImpl(bundles, resources, boundaries.storage,
                        new AIAssetEventPublisher(JSON) {
                            @Override
                            public void publishFileDeleteEvent(List<String> keys) {
                                synchronized (boundaries.deleted) { boundaries.deleted.addAll(keys); }
                            }
                        });
                real.initUploadAssets(AssetUploadInitRequest.builder().resourceId(resourceId).draftVersion(1)
                        .assets(List.of("SKILL.md", "notes.md").stream().map(name ->
                                AssetUploadInitRequest.AssetUploadRequest.builder().path("/").name(name)
                                        .assetResourceType(AssetResourceType.MD).expectedSize(8L).build()).toList()).build());
                var uploading = bundles.findByResourceIdAndVersion(resourceId, 1).orElseThrow();
                check("old-body".equals(uploading.getAssets().getFirst().getObjectKey()), "真实 Mongo 初始化保留旧正文");
                check(uploading.getRevision() == 1 && uploading.getPendingAssets().size() == 2,
                        "真实 Mongo 可在缺省修订号文档执行条件更新");
                check(bundles.updateDraftAssets(uploading.getId(), null, List.of(), List.of(), LocalDateTime.now()) == 0,
                        "真实 Mongo 拒绝过期修订号");
                CompletableFuture.allOf(List.of("new-1", "new-2").stream().map(key ->
                        CompletableFuture.runAsync(() -> real.handleFileUploaded(FileUploadedMessage.builder()
                                .scene(StorageSceneEnum.PRIVATE_SKILL_ASSET).objectKey(key).size(8L).build())))
                        .toArray(CompletableFuture[]::new)).join();
                var completed = real.getVersionBundle(resourceId, 1);
                check(completed.getAssets().size() == 2 && completed.getPendingAssets().isEmpty(),
                        "真实 Mongo 并发回调保留全部文件");
                real.cancelUploadAssets(AssetUploadCancelRequest.builder().resourceId(resourceId).draftVersion(1)
                        .objectKeys(List.of("new-1")).build());
                check(!boundaries.deleted.contains("new-1"), "真实 Mongo 迟到取消不删除成功文件");
                real.publishVersion(resourceId);
                check(resources.findByResourceId(resourceId).orElseThrow().getVersion() == 1,
                        "真实 Mongo 推进资源版本");
                resources.updateVersionByResourceId(resourceId, 0, LocalDateTime.now());
                check(resources.findByResourceId(resourceId).orElseThrow().getVersion() == 1,
                        "慢请求不得回退已发布版本");
                CompletableFuture.allOf(CompletableFuture.runAsync(() -> real.createDraftVersion(resourceId, 3)),
                        CompletableFuture.runAsync(() -> real.createDraftVersion(resourceId, 3))).join();
                check(bundles.findByResourceId(resourceId).size() == 3, "并发创建草稿不产生重复版本");
                var agent = agents.insert(AgentVersionBundleEntity.builder().resourceId(resourceId).version(1)
                        .status(VersionStatus.DRAFT).assets(completed.getAssets()).build());
                check(agents.updateDraftSpec(agent.getId(), AgentSpecInfoBase.builder().systemPrompt("check").build(),
                        LocalDateTime.now()) == 1, "旧 Agent 文档可局部更新配置");
                var configured = agents.findById(agent.getId()).orElseThrow();
                check(configured.getRevision() == 1 && configured.getAssets().size() == 2,
                        "Agent 配置局部更新递增修订号且不覆盖文件");
                System.out.println("PASS: real MongoDB legacy documents, CAS, concurrent callbacks, cancellation, publication and Agent spec");
            } finally {
                mongo.getDb().drop();
            }
        }
    }
}
