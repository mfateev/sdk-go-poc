# Temporal SDK unit-test porting checklist

Source checkout: `b87b0a1d5dda22d68ddb97237fdc42cd2492043f`. Runtime dependency: `go.temporal.io/sdk v1.49.0`, replaced by `github.com/mfateev/temporal-go-sdk v1.49.0-isolates.1.0.20261012175541-b87b0a1d5dda`.

This list inventories every named unit test and suite test method in the source checkout, including nested contrib modules. The separate `test/` integration module, testdata, generated non-test helpers, and build directories are excluded. Live dev-server scenarios found in contrib test files are listed and marked out of scope.

`internal/testports/inventory.json` is the source of truth. This document is generated from it. Every source test has a disposition and a reason; native coverage entries link to runnable tests. A consolidated native test may cover several original cases. Native goroutines, channels, context, select, timers and sync.WaitGroup replace the old workflow concurrency facade, so those ports assert native Go semantics.

Statuses:

- **covered**: assertions are mapped to new SDK unit tests or compiled isolate checks.
- **retained**: implementation and original tests remain in the delegated host SDK; listed native tests are supplemental boundary coverage.
- **blocked**: a required feature is missing, with a recorded user question or deferral. These entries are not counted as completed ports.
- **obsolete**: the removed facade or an explicitly excluded feature no longer has the original contract; replacement coverage is linked where applicable.
- **out_of_scope**: unshipped optional modules, build/release tools, or live server integration scenarios.
- **suite**: suite entry point whose individual methods are listed separately.

| Disposition | Source tests |
| --- | ---: |
| covered | 243 |
| retained | 857 |
| blocked | 0 |
| obsolete | 39 |
| out_of_scope | 332 |
| suite | 26 |
| pending | 0 |
| Total | 1497 |

## Feature decisions

The initial feature group was deferred by the user on 2026-10-11. Subsequently selected context propagation, data converters, custom failure conversion, generated protobuf values, corrupt-signal handling, stack-trace queries, native dynamic workflow/query/update handlers, named independent random streams with upstream golden semantics, native workflow streams and the isolate test environment are implemented. Other questions remain marked `awaiting_user` until answered; deferred tests stay blocked. No missing feature is implicitly treated as a completed port.

The isolate test facade reuses the host SDK test engine. Its original activity, timeout, retry, child scheduling and tracing assertion suites remain retained; linked native tests supplement those assertions. Direct ports cover the facade's native mock, listener, registration, ordering and completed-query behavior. Retained entries do not claim exhaustive duplication of every original assertion inside an isolate.

| Feature | Decision | Blocked tests |
| --- | --- | ---: |
| Workflow context propagators and native context-aware/per-context data converters. | implemented | 0 |
| Custom failure conversion at the isolate workflow boundary. | implemented | 0 |
| Native dynamic workflow/query/update handlers. | implemented | 0 |
| External-storage payload references at the isolate boundary (host middleware remains unchanged). | awaiting_user | 0 |
| Skip corrupt signals and log/metric them instead of failing a task. | implemented | 0 |
| Named independent workflow random streams with upstream golden sequence semantics. | implemented | 0 |
| Generated Google protobuf values inside isolate workflow APIs (binary/JSON, private caches and read-only conversion). | implemented | 0 |
| Workflow-instance stack-trace query API using native goroutine stacks. | implemented | 0 |
| Isolate-aware TestWorkflowEnvironment, native workflow mocks and replacements, child-start listeners, completed root/child queries, virtual time, and typed host-backed test references. | implemented | 0 |
| Variadic workflow/query/update handlers. | user_deferred | 0 |
| Native workflow stream handler namespace and subscription APIs. | implemented | 0 |

## Enforcement and verification

Run from this repository with the custom toolchain:

```sh
../golang-go/bin/tgo test ./...
../golang-go/bin/tgo run ./internal/cmd/testports -upstream ../temporal-go-sdk
```

The normal test suite rejects unfinished ports, missing reasons, duplicate entries, unpinned source files, missing runnable coverage targets, stale checklist text, and an SDK dependency version change. Comparing the source checkout also rejects new/unlisted tests, removed tests, and changed source test files. Run that comparison whenever upgrading or changing the upstream SDK.

Full feature parity remains incomplete while blockers exist. To require that stricter condition:

```sh
../golang-go/bin/tgo run ./internal/cmd/testports -upstream ../temporal-go-sdk -require-no-blocked
```

Refresh this document after reviewing and updating the JSON inventory:

```sh
../golang-go/bin/tgo run ./internal/cmd/testports -write-checklist
```

Retained host tests can be run through the upstream repository's canonical runner:

```sh
cd ../temporal-go-sdk/internal/cmd/build
TEMPORAL_GO_COMMAND="$(cd ../../../../golang-go/bin && pwd)/tgo" go run . unit-test
```

## Source-test list

### contrib/aws/lambdaworker/defaults_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestApplyLambdaWorkerDefaults_PreservesExisting` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestApplyLambdaClientDefaults` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestApplyLambdaClientDefaults_PreservesExistingLogger` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestBuildLambdaIdentity` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestLambdaDefaultConfigFilePath` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/aws/lambdaworker/options_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestConfigureWorkerContext_ReplayRegistrations` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/aws/lambdaworker/otel/otel_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestResolveServiceName` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestResolveEndpoint` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestApplyMetricsSetsHandler` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestApplyTracingAppendsInterceptor` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestApplyDefaultsWithProvidersWiring` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/aws/lambdaworker/runworker_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestRunWorkerInternal_Success` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_ConfigureCallbackError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_LoadConfigError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_DialError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_DialError_HandlerReturnsError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_MissingTaskQueue` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_MissingVersion` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_WorkerStartError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_UserOverridesApplied` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_LambdaDefaultsApplied` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_SetsCacheSize` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_CleanupPerInvocation` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_TaskQueueFromSetTaskQueue` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_IdentityFromLambdaContext` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_IdentityUserOverrideWins` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_IdentityNoLambdaContext` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_OnShutdownCalled` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_OnShutdownCalledPerInvocation` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_OnShutdownMultipleFuncs` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_OnShutdownErrorLogged` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_OnShutdownReceivesFreshContext` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_OnShutdownRunsAfterWorkerStop` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_TightDeadlineReturnsError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_TightDeadlineLogsWarning` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunWorkerInternal_PerInvocationLifecycle` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/aws/s3driver/awssdkv2/client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestAwsSdkClient_PutGetRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_ObjectExists` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_GetObject_NotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_PutObject_BucketNotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_ObjectExists_BucketNotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_GetObject_BucketNotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_LargeObject` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_Describe_ReturnsClientRegion` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_Describe_EmptyRegion` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAwsSdkClient_FullDriverRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/aws/s3driver/driver_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNewS3StorageDriver_Defaults` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewS3StorageDriver_CustomName` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewS3StorageDriver_NilClient` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewS3StorageDriver_NilBucketFunc` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewS3StorageDriver_NegativeMaxPayloadSize` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStaticBucket` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_SinglePayload` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_EmptyPayloads` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_Deduplication` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_MultiplePayloads` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_MaxPayloadSizeExceeded` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_DynamicBucket` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_PutObjectError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_ObjectExistsError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_RoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_MultiplePayloads` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_HashVerificationFailure` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_UnsupportedHashAlgorithm` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_MissingKey` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingBucket` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingKey` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_NilClaimData` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_GetObjectError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingHashAlgorithm` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingHashValue` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_NoTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_WorkflowInfo` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_ActivityInfo` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_WorkflowInfo_EmptyFields` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_ActivityInfo_EmptyFields` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_WorkflowInfo_SpecialChars` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_PreservesS3SafeSpecialChars` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_EscapesTilde` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_EscapesNonASCIIAsUTF8Bytes` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_WorkflowSpecExample` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectKey_ActivitySpecExample` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestSha256Hex` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_WithWorkflowTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_WithActivityTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_RoundTrip_WithWorkflowTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_DifferentTargets_SamePayload_DifferentKeys` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_EmptyDescribe` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_NilDescribe` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_SingleEntry` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_MultipleEntries` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/datadog/tracing/interceptor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSpanPropagation` | retained | `test_environment`: Tracing implementation and its full original span/log assertion suite remain in the delegated contrib SDK. The native test environment is now available; the linked native tracing tests are supplemental transport coverage and do not duplicate this full span-tree assertion. [TestNativeTracingInterceptors](example/tracing/check/main_test.go) [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `TestSpanName` | covered | Direct port of temporal.<operation> span naming. [TestUpstreamSpanName](contrib/datadog/tracing/interceptor_test.go) |
| `Test_tracerImpl_genSpanID` | obsolete | Upstream FNV run-ID golden values are replaced by replay-seeded isolate randomness; native IDs and repeated replay headers are covered. [TestNativeTracingInterceptors](example/tracing/check/main_test.go) [TestCompiledDeterministicRandom](example/random/check/main_test.go) |
| `Test_OnFinishOption` | covered | OnFinish moved to the host sink exporter. The port asserts callback error, custom finish options, timestamps and error suppression. [TestUpstreamOnFinishOption](contrib/datadog/exporter/exporter_test.go) |
| `Test_SpanFromWorkflowContext` | retained | `test_environment`: Tracing implementation and its full original span/log assertion suite remain in the delegated contrib SDK. The native test environment is now available; the linked native tracing tests are supplemental transport coverage and do not duplicate this full span-tree assertion. [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |

### contrib/envconfig/client_config_load_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLoadClientOptionsFile` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestLoadClientOptionsAPIKeyTLS` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestClientProfileApplyEnvVars` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestLoadDefaultConfigFileNotExist` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/envconfig/client_config_toml_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestClientConfigTOMLFull` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestClientConfigTOMLStrict` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestClientConfigTOMLPartial` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestClientConfigTOMLEmpty` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestClientConfigTOMLAdditionalProfileFields` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestClientConfigTOMLAdditionalProfileFieldsConflict` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestKnownProfileKeysInSync` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/gcp/cloudrun/otel/plugin_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPluginDefaults` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginResolutionPrecedence` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginIgnoresEmptyEnvironmentValues` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginConfiguresClient` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginForceFlushesProviders` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginDefersWorkerStopFlushByDefault` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginCanFlushOnWorkerStop` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginValidatesOptions` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginFlushHookError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/gcp/gcsdriver/driver_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNewGCSStorageDriver_Defaults` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewGCSStorageDriver_CustomName` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewGCSStorageDriver_NilClient` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewGCSStorageDriver_NilBucketFunc` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNewGCSStorageDriver_NegativeMaxPayloadSize` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStaticBucket` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_SinglePayload` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_WritesLegacyObjectNameClaim` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_EmptyPayloads` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_Deduplication` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_MultiplePayloads` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_MaxPayloadSizeExceeded` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_DynamicBucket` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_PutObjectError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_ObjectExistsError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_RoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_MultiplePayloads` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_HashVerificationFailure` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_UnsupportedHashAlgorithm` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ObjectNotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingBucket` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingObjectName` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_LegacyObjectNameClaim` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_NilClaimData` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_GetObjectError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingHashAlgorithm` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ClaimMissingHashValue` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectName_NoTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectName_WorkflowInfo` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectName_ActivityInfo` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectName_WorkflowInfo_EmptyFields` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectName_ActivityInfo_EmptyFields` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectName_WorkflowInfo_SpecialChars` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestObjectName_LongNameFallback` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_Empty` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_Plain` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_Slash` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_Percent` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_UnsafeChars` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_ControlChars` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_Dot` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_Unicode` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestEncodeObjectNameSegment_SpacesAndSymbols` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestSha256Hex` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_WithWorkflowTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_WithActivityTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_RoundTrip_WithWorkflowTarget` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_DifferentTargets_SamePayload_DifferentObjectNames` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_EmptyDescribe` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_NilDescribe` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_SingleEntry` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDescribeClient_MultipleEntries` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_ConcurrencyLimitedTo10` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStore_ContextCancellation` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRetrieve_ContextCancellation` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/gcp/gcsdriver/gcssdk/client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestGcsSdkClient_PutGetRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_ObjectExists` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_GetObject_NotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_PutObject_BucketNotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_ObjectExists_BucketNotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_GetObject_BucketNotFound` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_LargeObject` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_Describe` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGcsSdkClient_FullDriverRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/aggregate_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestAggregateResponsesConcatenatesText` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAggregateResponsesCopiesFirst` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAggregateResponsesFoldsMetadata` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAggregateResponsesNilNext` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestClassifyModelErrorRetryability` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAggregateResponsesFoldsAllLateFields` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/aggregate_terminal_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestAggregateResponsesGeminiTerminalAuthoritative` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAggregateResponsesTerminalReplaceCopies` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestAggregateResponsesKeepsMultipleAuthoritative` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/context_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestConcurrentFanoutSchedulesParallel` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestSequentialFanoutSchedulesSerially` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDeterministicTimeProvider` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDeterministicUUIDProvider` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/continueasnew_scopes_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestExportSessionStripsNonSessionScopedEventDeltas` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/continueasnew_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestContinueAsNewRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/converter_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLLMRequestResponseRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/gemini_e2e_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestGeminiModelEndToEnd` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGeminiModelErrorIsNonRetryable` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/heartbeat_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestModelHeartbeatsWhileAwaitingSlowResponse` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStreamingModelHeartbeatsBeforeFirstChunk` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/hitl_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestHITLConfirmationResumesTool` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestHITLMultiDecisionBatchedResume` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/integration_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestStreamingIntegration` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestReplaySingleAndMultiAgent` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/internal/pluginwarn/pluginwarn_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPluginWarnsOnRawGlobalProvider` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/mcp_closed_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestMcpToolsetClosedIsTerminal` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/mcp_hitl_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestMCPToolConfirmationPausesAndResumes` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestMCPToolConfirmationDenialBlocksTool` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/mcp_lifecycle_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestMcpToolsetConstructedOncePerWorker` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivitiesCloseClosesCachedToolset` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestMcpFactoryErrorNotCached` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/mcp_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestMcpListToolsCarriesParameters` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestMcpToolCallRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/model_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSingleAgentRoutesModelToActivity` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestModelFactoryLookupByName` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPerModelTimeoutApplied` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestSummaryFnOnModelActivity` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDefaultSummaryIsAgentName` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestModelResolvedViaRegistry` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/nested_fanout_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNestedFanoutJoinsOnInvokingCoroutine` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNestedAgentToolFanout` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/plugin_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNewPluginRejectsInvalidConfig` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginRegistersActivitiesOnWorkerStart` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginClosesMCPToolsetsOnWorkerStop` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPluginReplayDoesNotCloseToolsets` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/registry_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestGeminiResolvesOutOfTheBox` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestNonGeminiUnknownNameStillErrors` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGeminiRegistrationIsCallerOwned` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/run_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestMultiAgentSubAgents` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestModelErrorClassification` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestToolErrorFedBackToModel` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/streaming_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNonStreamingDoesNotStream` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/telemetry_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestIsRawOTelSDKProvider` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestWarnOnNonReplaySafeTelemetryProviders` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/telemetry_liveops_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestQueryHandlerGateOnCommandFreeHistory` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestQueryHandlerGateWithPendingCommandOrCompletion` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestUpdateValidatorTelemetryIsNeverReplaySuppressed` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/telemetry_otelv2_probe_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSpanIDsDeterministicAcrossReplay` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStraddleSpanExportedOnceComplete` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestTaskRetryReexportsSpan` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestDerivedTracerSpansGatedAndDeterministic` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestStraddleSpanLegacyControl` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/telemetry_surface_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestReplaySafeWrapperSurface` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestWorkflowSpanTracerProviderReturnsOwnedProvider` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestSurfaceGuardDetectsSimulatedInterfaceGrowth` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/telemetry_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestReplayTelemetry` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestReplaySafeProvidersPassThroughNonWorkflowContext` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestReplaySafeMeterWrapsInstrumentReturnedWithError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestReplaySafeMeterObservablePassthrough` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestReplaySafeProvidersRecordDuringLiveWorkflow` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/tool_schema_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestActivityAsToolSchemaExcludesJSONDashFields` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/tool_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestFunctionToolRunsInWorkflow` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolRoundTrip` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestInWorkflowToolMutatesState` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/googleadk/tool_validation_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestActivityAsToolRejectsMissingContextArg` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolRejectsSingleInput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolRejectsExtraInputs` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolRejectsWorkflowContextArg` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolRejectsNarrowerContextArg` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolRejectsNoReturnValues` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolRejectsNonErrorLastReturn` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolAllowsErrorOnlyReturn` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestActivityAsToolAcceptsPointerArgs` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/opentelemetry/handler_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestTags` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCounterHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCounterHandlerWithMonotonicCounters` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGaugeHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestTimerHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### contrib/opentelemetry/otlpworker/apply_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestApplyMetrics` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestApplyTracing` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestApplyInstallsBoth` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |

### contrib/opentelemetry/otlpworker/flush_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestForceFlush` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestForceFlushJoinsErrors` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestShutdown` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestFlushNoProviders` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |

### contrib/opentelemetry/otlpworker/providers_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestBuildResourceIncludesServiceName` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestBuildResourceAppliesResourceOptions` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestNewProvidersConstructs` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |
| `TestNewProvidersEndpointURLMode` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |

### contrib/opentelemetry/otlpworker/resolve_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestFirstNonEmptyEnv` | out_of_scope | Optional OTLP worker provider configuration is not shipped by this SDK; upstream module remains independent. |

### contrib/opentelemetry/tracing_interceptor_logger_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLogFields` | covered | New tracer GetLogger emits the original exact TraceID/SpanID fields; host activity logging remains upstream. [TestUpstreamLogFields](contrib/opentelemetry/upstream_test.go) |

### contrib/opentelemetry/tracing_interceptor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSpanPropagation` | retained | `test_environment`: Tracing implementation and its full original span/log assertion suite remain in the delegated contrib SDK. The native test environment is now available; the linked native tracing tests are supplemental transport coverage and do not duplicate this full span-tree assertion. [TestNativeTracingInterceptors](example/tracing/check/main_test.go) [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `TestSpanKind` | covered | Ported server/client span kinds and regular/benign error status with the new tracer. [TestUpstreamSpanKindStatusAndTimestamps](contrib/opentelemetry/upstream_test.go) |
| `TestBenignErrorSpanStatus` | covered | Ported server/client span kinds and regular/benign error status with the new tracer. [TestUpstreamSpanKindStatusAndTimestamps](contrib/opentelemetry/upstream_test.go) |
| `TestSpanFromWorkflowContext` | covered | Ported valid native context extraction and invalid private no-op fallback. A process-global no-op object is intentionally not shared. [TestUpstreamSpanFromWorkflowContext](contrib/opentelemetry/upstream_test.go) [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |
| `TestSpanFromWorkflowContextNoOpSpan` | covered | Ported valid native context extraction and invalid private no-op fallback. A process-global no-op object is intentionally not shared. [TestUpstreamSpanFromWorkflowContext](contrib/opentelemetry/upstream_test.go) [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |

### contrib/opentelemetry-v2/codec_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestCodecTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `codecTestSuite.TestMarshalSpan` | covered | Ported success, malformed span/header, disabled baggage, span-only, baggage-only and empty carrier cases. [TestUpstreamSpanCodec](contrib/opentelemetry-v2/upstream_test.go) |
| `codecTestSuite.TestUnmarshalSpan` | covered | Ported success, malformed span/header, disabled baggage, span-only, baggage-only and empty carrier cases. [TestUpstreamSpanCodec](contrib/opentelemetry-v2/upstream_test.go) |
| `codecTestSuite.TestTextMapCarrierGetIsCaseInsensitive` | covered | Ported case-insensitive carrier reads and case-preserving writes. [TestUpstreamTextMapCarrier](contrib/opentelemetry-v2/upstream_test.go) |
| `codecTestSuite.TestTextMapCarrierSetPreservesCase` | covered | Ported case-insensitive carrier reads and case-preserving writes. [TestUpstreamTextMapCarrier](contrib/opentelemetry-v2/upstream_test.go) |

### contrib/opentelemetry-v2/context_bridge_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestContextBridgeTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `contextBridgeTestSuite.TestKeepsAmbientBaggage` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |
| `contextBridgeTestSuite.TestContextWithParentDefaultsToBaggageEnabled` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |
| `contextBridgeTestSuite.TestReplacesAmbientBaggageWithSpanBaggage` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |
| `contextBridgeTestSuite.TestNoSpanOnContextReturnsNoopSpan` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |
| `contextBridgeTestSuite.TestUnsupportedSpanTypeReturnsNoopSpan` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |
| `contextBridgeTestSuite.TestNilSpanKeepsStoredSpan` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |
| `contextBridgeTestSuite.TestUnknownSpanTypeKeepsStoredSpan` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |
| `contextBridgeTestSuite.TestWorkflowContextWithSpanStoresKnownType` | covered | Direct port of baggage inheritance/replacement, private no-op spans, and nil/unknown/stored span context cases. [TestUpstreamContextBridge](contrib/opentelemetry-v2/upstream_test.go) |

### contrib/opentelemetry-v2/interceptor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestInterceptorTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `interceptorTestSuite.TestSpanKind` | covered | Native v2 CreateSpan and Finish assert inbound/outbound kinds and regular/benign error status. Host RunActivity behavior stays upstream. [TestUpstreamSpanKindAndErrorStatus](contrib/opentelemetry-v2/upstream_test.go) [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |
| `interceptorTestSuite.TestSpanErrorStatus` | covered | Native v2 CreateSpan and Finish assert inbound/outbound kinds and regular/benign error status. Host RunActivity behavior stays upstream. [TestUpstreamSpanKindAndErrorStatus](contrib/opentelemetry-v2/upstream_test.go) [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |

### contrib/opentelemetry-v2/interceptor_tracer_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestIntegrationTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `integrationTestSuite.TestComprehensive` | out_of_scope | Live dev-server comprehensive client/worker/Nexus integration scenario, including standalone Nexus server flags. Native tracing unit/compiled checks are tracked separately. |

### contrib/opentelemetry-v2/logger_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLoggerTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `loggerTestSuite.TestGetLoggerAddsValidSpan` | covered | Ported exact TraceID/SpanID logging and invalid/no-span suppression. [TestUpstreamGetLogger](contrib/opentelemetry-v2/upstream_test.go) |
| `loggerTestSuite.TestGetLoggerSkipsInvalidSpan` | covered | Ported exact TraceID/SpanID logging and invalid/no-span suppression. [TestUpstreamGetLogger](contrib/opentelemetry-v2/upstream_test.go) |
| `loggerTestSuite.TestGetLoggerTraceFields` | retained | `test_environment`: Tracing implementation and its full original span/log assertion suite remain in the delegated contrib SDK. The native test environment is now available; the linked native tracing tests are supplemental transport coverage and do not duplicate this full span-tree assertion. [TestUpstreamGetLogger](contrib/opentelemetry-v2/upstream_test.go) |
| `loggerTestSuite.TestNexusHandlerLoggerTraceFields` | out_of_scope | This is a live dev-server Nexus handler integration test. Nexus workers/logging remain in the upstream host plugin, outside native workflow unit ports. |

### contrib/opentelemetry-v2/metrics_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestTags` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCounterHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCounterHandlerWithMonotonicCounters` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGaugeHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestTimerHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### contrib/opentelemetry-v2/provider_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestReplaySafeTracerProviderValidation` | obsolete | New tracing requires a private factory-owned provider rather than process-global otel.SetTracerProvider/NewPlugin validation. [TestInterceptorConfigurationValidation](worker/interceptor_test.go) [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |

### contrib/opentelemetry-v2/tracer_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestTracerIntegrationTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `tracerIntegrationTestSuite.TestTracerWorkflow` | out_of_scope | These suite methods run live dev-server/reset/retry/continue-as-new integration scenarios, not unit tests. Existing native tracing replay fixtures remain separately covered. |
| `tracerIntegrationTestSuite.TestContinueAsNewUnderUserSpan` | out_of_scope | These suite methods run live dev-server/reset/retry/continue-as-new integration scenarios, not unit tests. Existing native tracing replay fixtures remain separately covered. |
| `tracerIntegrationTestSuite.TestWorkflowTaskRetryReusesSpanID` | out_of_scope | These suite methods run live dev-server/reset/retry/continue-as-new integration scenarios, not unit tests. Existing native tracing replay fixtures remain separately covered. |
| `tracerIntegrationTestSuite.TestTracerReset` | out_of_scope | These suite methods run live dev-server/reset/retry/continue-as-new integration scenarios, not unit tests. Existing native tracing replay fixtures remain separately covered. |

### contrib/opentracing/interceptor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSpanPropagation` | retained | `test_environment`: Tracing implementation and its full original span/log assertion suite remain in the delegated contrib SDK. The native test environment is now available; the linked native tracing tests are supplemental transport coverage and do not duplicate this full span-tree assertion. [TestNativeTracingInterceptors](example/tracing/check/main_test.go) [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |

### contrib/sysinfo/cgroups_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestHandleCGroupUpdateError` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/sysinfo/sysinfo_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestGetMemoryCpuUsage` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestMaybeRefreshRateLimiting` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/tally/handler_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestTally` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/tally/prometheus_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPrometheusNaming` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/tools/workflowcheck/determinism/determinism_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/tools/workflowcheck/workflow/workflow_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### contrib/workflowstreams/client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestFlushSendsBufferedItems` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestFlushSendsBufferedItems](contrib/workflowstreams/client_test.go) |
| `TestPayloadConvertersDriveItemConversion` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestPayloadConvertersDriveItemConversion](contrib/workflowstreams/client_test.go) |
| `TestFlushNoopWhenEmpty` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestFlushNoopWhenEmpty](contrib/workflowstreams/client_test.go) |
| `TestSequenceAdvancesAcrossFlushes` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSequenceAdvancesAcrossFlushes](contrib/workflowstreams/client_test.go) |
| `TestMaxBatchSizeTriggersFlush` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestMaxBatchSizeTriggersFlush](contrib/workflowstreams/client_test.go) |
| `TestCloseDrainsBuffer` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestCloseDrainsBuffer](contrib/workflowstreams/client_test.go) |
| `TestForceFlush` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestForceFlush](contrib/workflowstreams/client_test.go) |
| `TestFlushTimeoutAfterMaxRetryDuration` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestFlushTimeoutAfterMaxRetryDuration](contrib/workflowstreams/client_test.go) |

### contrib/workflowstreams/codec_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPayloadWireRoundTrip` | covered | `workflow_streams`: Direct upstream wire-codec port preserving base64 protobuf interoperability and payload encoding assertions. [TestPayloadWireRoundTrip](contrib/workflowstreams/codec_test.go) |
| `TestPayloadWireFormatIsBase64OfProto` | covered | `workflow_streams`: Direct upstream wire-codec port preserving base64 protobuf interoperability and payload encoding assertions. [TestPayloadWireFormatIsBase64OfProto](contrib/workflowstreams/codec_test.go) |
| `TestDecodePayloadWireRejectsBadBase64` | covered | `workflow_streams`: Direct upstream wire-codec port preserving base64 protobuf interoperability and payload encoding assertions. [TestDecodePayloadWireRejectsBadBase64](contrib/workflowstreams/codec_test.go) |
| `TestBinaryPayloadRoundTrip` | covered | `workflow_streams`: Direct upstream wire-codec port preserving base64 protobuf interoperability and payload encoding assertions. [TestBinaryPayloadRoundTrip](contrib/workflowstreams/codec_test.go) |

### contrib/workflowstreams/subscribe_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSubscribeDeliversItemsAndAdvancesOffset` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeDeliversItemsAndAdvancesOffset](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeTruncationResetsOffset` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeTruncationResetsOffset](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeTerminalEndsCleanly` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeTerminalEndsCleanly](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeContinueAsNewRetries` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeContinueAsNewRetries](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeRetriesWhileDraining` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeRetriesWhileDraining](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeSurfacesNonTerminalError` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeSurfacesNonTerminalError](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeContextCanceledBeforePolling` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeContextCanceledBeforePolling](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeCooldownCanceledByContext` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeCooldownCanceledByContext](contrib/workflowstreams/subscribe_test.go) |
| `TestSubscribeCooldownAppliedWhenNotMoreReady` | covered | `workflow_streams`: Direct upstream host publisher/subscription port against the native stream package; fake clients preserve batching, error, cancellation and rollover assertions. [TestSubscribeCooldownAppliedWhenNotMoreReady](contrib/workflowstreams/subscribe_test.go) |

### contrib/workflowstreams/workflow_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWorkflowPublishUsesConfiguredConverters` | covered | `workflow_streams`: Direct upstream port using the native stream package; workflow handlers execute in compiled isolates with context.Context and native channels. [TestWorkflowPublishUsesConfiguredConverters](contrib/workflowstreams/workflow_test.go) |
| `TestExternalPublishAndOffsetQuery` | covered | `workflow_streams`: Direct upstream port using the native stream package; workflow handlers execute in compiled isolates with context.Context and native channels. [TestExternalPublishAndOffsetQuery](contrib/workflowstreams/workflow_test.go) |
| `TestPublisherDedup` | covered | `workflow_streams`: Direct upstream port using the native stream package; workflow handlers execute in compiled isolates with context.Context and native channels. [TestPublisherDedup](contrib/workflowstreams/workflow_test.go) |
| `TestPollReturnsItemsWithTopicFilter` | covered | `workflow_streams`: Direct upstream port using the native stream package; workflow handlers execute in compiled isolates with context.Context and native channels. [TestPollReturnsItemsWithTopicFilter](contrib/workflowstreams/workflow_test.go) |

### converter/byte_slice_payload_converter_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestByteSliceConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### converter/codec_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestEncodingDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadCodecHTTPHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRemoteDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRawValueCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRawValueJsonConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCodecDataConverter_ToPayload_EncodeError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### converter/composite_data_converter_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestRawValueCompositeDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCompositeDataConverter_MixedValues` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### converter/data_converter_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestDefaultDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestToStrings` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### converter/grpc_interceptor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPayloadCodecGRPCClientInterceptor` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestFailureGRPCClientInterceptor` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### converter/payload_converter_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestProtoJsonPayloadConverter_Google` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoJsonPayloadConverter_Gogo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoPayloadConverter_Google` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoPayloadConverter_Gogo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestJsonPayloadConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoJsonPayloadConverter_Nil` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestJsonPayloadConverter_Nil` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNilPayloadConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoPayloadConverter_WithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoJSONPayloadConverter_WithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoJsonPayloadConverter_FromPayload_Errors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestProtoPayloadConverter_FromPayload_Errors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestJsonPayloadConverter_FromPayload_Errors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### converter/payload_handler_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestRouting_WrongMethod` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRouting_WrongPath` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRouting_NilBody` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRouting_MountedAtPrefix` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_AppliesPostThenPreCodecs` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_NoDrivers_StorageRefFails` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_NoDrivers_MixedBatch_StorageRefFails` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_RoundTrip` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_PreserveStorageRefs_AllRefs` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_PreserveStorageRefs_NoRefs` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_PreserveStorageRefs_MixedBatch` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_PreserveStorageRefs_CaseInsensitive` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecode_PreserveStorageRefs_False` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDownload_SingleRef` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDownload_MultipleRefs` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDownload_NonStorageRef` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDownload_NoDrivers` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDownload_UnknownDriver` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDownload_MultipleDrivers` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncode_NoCodecsNoStorage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncode_PreStorageCodecsOnly` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncode_PostStorageCodecsOnly` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncode_PreAndPostCodecs` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncode_WithExternalStorage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncode_DecodeRoundTrip` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### converter/serialization_context_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestCodecDataConverter_PropagatesBothDCAndCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCodecDataConverter_PropagatesCodecOnly` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCodecDataConverter_PropagatesDCOnly` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCodecDataConverter_NeitherImplements` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCodecDataConverter_SigningMismatchFailsDecode_WorkflowContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCodecDataConverter_SigningMismatchFailsDecode_ActivityContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCodecDataConverter_NexusSerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusSerializationContextReachesDataAndFailureConverters` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWithSerializationContext_NilReturnPanics` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWithFailureConverterSerializationContext_NilReturnPanics` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDefaultDataConverter_NotSerializationContextAware` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWithSerializationContextHelper_Wraps` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### interceptor/tracing/tracing_interceptor_v2_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWriteSpanHeaderFinishesSpanOnError` | covered | Exact header-write failure/finish callback/error identity assertions ported to native tracing. [TestUpstreamWriteSpanHeaderFinishesSpanOnError](interceptor/tracing/upstream_test.go) |

### interceptor/tracing_interceptor_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestIdempotencyKeyGeneration` | covered | Assert native replay identity plus increasing span counter; explicit new contract uses OriginalRunID, with RunID fallback. [TestUpstreamIdempotencyKeyGeneration](interceptor/upstream_test.go) [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |

### interceptor/tracing_interceptor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSpanTimestamps` | covered | New v1 tracer preserves supplied workflow timestamps; native compiled spans use deterministic time. [TestUpstreamSpanKindStatusAndTimestamps](contrib/opentelemetry/upstream_test.go) [TestNativeTracingInterceptors](example/tracing/check/main_test.go) |

### internal/activity_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestActivityTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `activityTestSuite.TestActivityHeartbeat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestActivityHeartbeat_InternalError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestActivityHeartbeat_CancelRequested` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestActivityHeartbeat_PauseRequested` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestActivityHeartbeat_ResetRequested` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestActivityHeartbeat_EntityNotExist` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestActivityHeartbeat_SuppressContinousInvokes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestActivityHeartbeat_WorkerStop` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestGetWorkerStopChannel` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestIsActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `activityTestSuite.TestGetClient` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/cmd/build/main_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestCheckModuleDirs` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestFindModuleDirs` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestFindUnitModuleDirs` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestTestOutputFlagsDefaultToFailures` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPrepareTestOutput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestTestOutputWriters` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestGoTestJSONWriterAttributesFailedSubtestOutput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunTestCmdFailureOutput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunTestCmdPassingOutput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunTestCmdFullOutput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestWriteStructuredTestFailureReportFallsBackToCommandOutput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestWriteStructuredTestFailureReportListsAllFailuresWhenDetailsAreOmitted` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestWriteTestSetupFailureReportIncludesDevServerOutputAndLogLocations` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestCollectServerFailureContexts` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestWriteStructuredTestFailureReportIncludesArtifactAndServerContext` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRunTestCmdHelperProcess` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestRenderTestFailureSummaryEscapesHTML` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestCollapseParentFailureRowsUsesPackage` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestCollapseParentFailureRowsPreservesAmbiguousParentOutput` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### internal/cmd/build/testsuiteassertions/analyzer_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestAnalyzer` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### internal/cmd/prepare-release/main_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestCreateDraftRelease` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestOpenDraftPR` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPushBranch` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestCommitRelease` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestCreateDraftPRStopsBeforePush` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestUpdateFile` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestCreateWorktree` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestFetchMain` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestValidateReleaseFilesRejectsNonIncreasingVersion` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestValidateReleaseFilesRejectsInvalidChangelog` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPrepareDraftPRValidatesBeforeCreatingBranch` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPrepareDraftPRLeavesWorktreeAfterFailure` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPrepareDraftPRLeavesDirectoryIfWorktreeCreationFails` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestPrepareEverything` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### internal/cmd/prepare-release/purehelpers_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestRegularExpressions` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestValidateVersionIncrease` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestValidateGoMod` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestValidateChangelog` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestReplaceSDKVersion` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestUpdateChangelog` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestUpdateChangelogPreservesHistory` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestUpdateChangelogRejectsInvalidState` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### internal/cmd/tools/doclink/doclink_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestIsValidDefinitionWithMatchGroupedInterface` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestSupportsGoSyntax` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestIsValidDefinitionWithMatchGroupedNamedType` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestIsValidDefinitionWithMatchSkipsEmbeddedInterfaceMember` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestIsValidDefinitionWithMatchSkipsCommentedEmbeddedInterfaceMember` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestProcessInternalAddsDocLinkForGroupedInterface` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |
| `TestProcessInternalDoesNotAddDocLinkForCommentedEmbeddedInterfaceMember` | out_of_scope | Optional module or upstream build/release tooling is not replaced or shipped by this SDK. |

### internal/common/backoff/retry_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestRetrySuccess` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNoRetryAfterContextDone` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetryFailed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestIsRetryableSuccess` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestIsRetryableFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestConcurrentRetrier` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetryDeadlineExceeded` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/common/backoff/retrypolicy_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestExponentialBackoff` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNumberOfAttempts` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMaximumInterval` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestBackoffCoefficient` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExpirationInterval` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExpirationOverflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDefaultPublishRetryPolicy` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNoMaxAttempts` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestUnbounded` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/common/cache/lru_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLRU` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLRUWithTTL` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLRUCacheConcurrentAccess` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRemoveFunc` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRemovedFuncWithTTL` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClear` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLRUMax` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/common/metrics/grpc_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestGRPCInterceptor` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/common/metrics/handler_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestReplayAwareHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/common/retry/interceptor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestIsWorkflowTaskCompletionBufferLost` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/common/util/stringer_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_byteSliceToString` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestValueToString` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/context_aware_data_converter_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestContextAwareDataConverter` | covered | `context_propagation`: Native WithContext/WithWorkflowContext helpers preserve context-aware masking and parent isolation; compiled executions verify worker-configured context binding, scratch queries/validators and operation result decoding. [TestUpstreamNativeContextAwareDataConverter](workflow/context_converter_test.go) [TestUpstreamNativeContextPropagation](example/contextpropagation/check/main_test.go) [TestNativeContextHistoryReplay](example/contextpropagation/check/main_test.go) |

### internal/credentials_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestAPIKeyCredentials_TLSEnabledByDefaultWhenAPIKeyProvided` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestAPIKeyCredentials_TLSCanBeExplicitlyDisabledWithAPIKey` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestConnectionOptions_TLSDisabledByDefaultWithoutAPIKey` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestAPIKeyCredentials_ExplicitTLSConfigPreservedWithAPIKey` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestConnectionOptions_TLSAndTLSDisabledMutuallyExclusive` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/dynamic_workflow_factory_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestGetWorkflowDefinition_DynamicWorkflowFactory` | retained | Custom WorkflowDefinitionFactory lookup, child dispatch and update draining remain in the delegated host SDK. Compiled native workflows additionally exercise the same factory path. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestDynamicWorkflowFactoryInTestEnvironment` | retained | Custom WorkflowDefinitionFactory lookup, child dispatch and update draining remain in the delegated host SDK. Compiled native workflows additionally exercise the same factory path. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestDynamicWorkflowFactoryChildInTestEnvironment` | retained | Custom WorkflowDefinitionFactory lookup, child dispatch and update draining remain in the delegated host SDK. Compiled native workflows additionally exercise the same factory path. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestDynamicWorkflowFactoryDrainsUnhandledUpdatesInTestEnvironment` | retained | Custom WorkflowDefinitionFactory lookup, child dispatch and update draining remain in the delegated host SDK. Compiled native workflows additionally exercise the same factory path. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |

### internal/encode_args_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestDecodeArg` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/error_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_GenericGoError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityErrorAccessors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityNotRegistered` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_TimeoutError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_TimeoutError_WithDetails` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ApplicationError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ApplicationError_Pointer` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_CanceledError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_IsCanceledError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_CanceledErrorWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_CanceledErrorWithOptions_RoundTrip` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestErrorDetailsValues` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_SignalExternalWorkflowExecutionFailedError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ContinueAsNewError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ContinueAsNewErrorWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_GetErrorType` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_IsRetryable` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_ApplicationError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_ApplicationErrorWithExtraRequests` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_EncodeMessage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_EncodeDecodeEncodeMessage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_EncodeStackTrace` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_CanceledError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_PanicError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_TimeoutError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_TerminateError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_ServerError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_ActivityError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_ChildWorkflowExecutionError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_NexusHandlerError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_UnknownError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertErrorToFailure_SavedFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertFailureToError_ApplicationFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertFailureToError_ResetWorkflowFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertFailureToError_CanceledFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertFailureToError_TimeoutFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertFailureToError_ServerFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_convertFailureToError_SaveFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestErrorToFailure_NexusOperationError_SetsTokenAndId` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestFailureToError_NexusOperationExecution_TokenFallbackToId` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHandlerError_EncodeCommonAttributes_RoundTrip` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHandlerError_EncodeCommonAttributes_MultipleRoundTrips` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/exposed_proto_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPollWorkflowUpdate_SuccessResult_GetPayloads` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPollWorkflowUpdate_FailureError_ExposesFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityResult_SuccessPayloads` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityResult_FailureExposesFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGetPayloads_NonImplementor` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGetPayloads_NilInput` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncodedValues_GetPayloads` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/extstore/extstore_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestExternalStorageToParams_NegativeThreshold` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExternalStorageToParams_ZeroThresholdUsesDefault` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExternalStorageToParams_ExplicitThreshold` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExternalStorageToParams_MultipleDriversRequireSelector` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExternalStorageToParams_DuplicateDriverNames` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExternalStorageToParams_PointerAndValueReceiverDrivers` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExternalStorageToParams_EmptyDrivers` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExternalStorageToParams_SingleDriverSynthesizesSelector` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStorageReferenceRoundTrip` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadToStorageReference_WrongEncoding` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadToStorageReference_CorruptJSON` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadToStorageReference_ProtoJSONFormat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadToStorageReference_LegacyFormat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadToStorageReference_ProtoJSON_WrongMessageType` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestIsStorageReference_ProtoJSONFormat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestIsStorageReference_LegacyFormat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestIsStorageReference_ProtoJSON_WrongMessageType` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestIsStorageReference_NotStorageReference` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_NoDriverNoop` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_BelowThreshold_NotStored` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_AtThreshold_Stored` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_AboveThreshold_Stored` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_MultiplePayloads_Batched` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_SelectorNil_PayloadInline` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_SelectorReceivesSelectContextWithTarget` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_SelectorBelowThreshold_NotCalled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_SelectorRoutes_TwoDrivers` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_SelectorUnregisteredDriver` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_SelectorError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_WrongClaimCount` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_StoreError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_StorePanic` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_CancelOnError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_Callback` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreVisitor_Callback_ExternalCountOnly` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_InlinePassthrough` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_Mixed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_BatchedByDriver` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_MultiDriver` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_UnknownDriver` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_RetrieveError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_RetrievePanic` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_CancelOnError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_WrongPayloadCount` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_Callback` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_Callback_ExternalCountOnly` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetrievalVisitor_LegacyFormat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClaimDeserialization_PlainJSON_OtherSdk` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClaimDeserialization_OtherSdk_ProtoJSON` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreRetrieveRoundTrip_Single` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreRetrieveRoundTrip_MixedInline` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStoreRetrieveRoundTrip_PointerAndValueReceiverDrivers` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/grpc_dialer_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestErrorWrapper_SimpleError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestErrorWrapper_ErrorWithFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeadersProvider_PopulateAuthToken` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeadersProvider_Error` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeadersProvider_NotIncludedWhenNil` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeadersProvider_IncludedWithHeadersProvider` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMissingGetServerInfo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestInternalErrorRetry` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerAndLazyClient` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCheckHealth` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDialOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGrpcCompression` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGrpcCompressionDowngradesUnsupportedMethod` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGrpcCompressionNoneDoesNotInstallDowngrade` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestGrpcCompressionDoesNotDowngradeGenericUnimplemented` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCustomResolver` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestResourceExhaustedCause` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCredentialsAPIKey` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExistingContextMetadataJoinedWithSDKHeaders` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNamespaceInterceptor` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCredentialsMTLS` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/headers_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestHeaderWriter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeaderReader_ForEachKey` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeaderReader_Get` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/interceptortest/proxy_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestProxy` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_activity_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestExecuteActivityHeaderAvailableToInterceptors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExecuteActivityFromLinklessNexusRequestOmitsOnConflictOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestUpdateActivityOptionsMask` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityOperatorCommandRequestFields` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestInterceptorReceivesCommandArguments` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestUpdateActivityOptionsRestoreIsExclusive` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExecuteActivityFromNexusRequestUsesStableRequestID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExecuteActivityValidationFailsBeforeStartRPC` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_command_state_machine_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_TimerStateMachine_CancelBeforeSent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_TimerStateMachine_CancelAfterInitiated` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_TimerStateMachine_CompletedAfterCancel` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_TimerStateMachine_CompleteWithoutCancel` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_TimerCancelEventOrdering` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_CompleteWithoutCancel` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_CancelBeforeSent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_CancelAfterSent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_CompletedAfterCancel` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_CompletionRemovesPendingCancelWithCustomActivityID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_CancelInitiated_After_CanceledBeforeSent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_PanicInvalidStateTransition` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ChildWorkflowStateMachine_Basic` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ChildWorkflowStateMachine_CancelSucceed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ChildWorkflowStateMachine_InvalidStates` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ChildWorkflow_UnusualCancelationOrdering` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ChildWorkflowStateMachine_CancelFailed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_MarkerStateMachine` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_UpsertSearchAttributesCommandStateMachine` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ModifyPropertiesCommandStateMachine` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_CancelExternalWorkflowStateMachine_Succeed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_CancelExternalWorkflowStateMachine_Failed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ActivityStateMachine_CancelInitiated_WhileInInitiatedState` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_coroutines_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestDispatcher` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestDispatcherDeferClose` | obsolete | These tests observe cooperative-dispatcher teardown defers mutating host-owned memory. Isolate eviction revokes execution ownership; host lifecycle and late-callback retirement are covered instead. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) [TestResourcesCompiled](example/resources/check/main_test.go) |
| `TestDispatcherDeadlockedDefer` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestDispatcherDeferCloseRace` | obsolete | These tests observe cooperative-dispatcher teardown defers mutating host-owned memory. Isolate eviction revokes execution ownership; host lifecycle and late-callback retirement are covered instead. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) [TestResourcesCompiled](example/resources/check/main_test.go) |
| `TestNonBlockingChildren` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestNonbufferedChannel` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBufferedChannelReceiveWithTimeout` | covered | Native receive/select timer scenarios check virtual time and context cancellation; Await helpers are replaced by native synchronization. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestUnbufferedChannelReceiveWithTimeout` | covered | Native receive/select timer scenarios check virtual time and context cancellation; Await helpers are replaced by native synchronization. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestNonbufferedChannelBlockedReceive` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBufferedChannelPut` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBufferedChannelGet` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestChannelName` | obsolete | Native Go channels have no SDK channel-name facade. Signal name/description registration retains separate coverage. [TestTypedSignalChannelIdentityOrderingAndCancellation](workflow/signal_test.go) |
| `TestNotBlockingSelect` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBlockingSelect` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSelectBlockingDefaultWithOldFlagBehavior` | obsolete | Legacy selector SDK flags do not apply to native select. Blocking default must preserve pending values in the new scenario. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSelectBlockingDefaultWithDefaultFlags` | obsolete | Legacy selector SDK flags do not apply to native select. Blocking default must preserve pending values in the new scenario. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBlockingSelectFuture` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestFutureToChannelWaitsAndSupportsIndependentTypedExtraction](workflow/activity_test.go) |
| `TestBlockingSelectSend` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBlockingSelectAsyncSend` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSelectOnClosedChannel` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBlockingSelectAsyncSend2` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSendSelect` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSendSelectWithAsyncReceive` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestChannelClose` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSendClosedChannel` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestBlockedSendClosedChannel` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestAsyncSendClosedChannel` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestDispatchClose` | covered | Ports the root plus ten blocked children, complete stack membership, root-only history and close/drain assertions through a compiled native workflow. Native goroutine headers and instance resource counts replace named coroutine three-line formatting and process-wide sleep-based counts. [TestDispatchClose](example/query/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestPanic` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestYieldDuringPanic` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestChannelReceivePointer` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestAwait` | covered | Native channel/context waits and WaitHandlersFinished replace condition polling. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestDeadlockDetectorAndAwaitRace` | obsolete | Removed cooperative coroutine detector internals; native workflow task deadlines and resource diagnostics have direct coverage. [TestResourcesCompiled](example/resources/check/main_test.go) |
| `TestAwaitCancellation` | covered | Native channel/context waits and WaitHandlersFinished replace condition polling. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestAwaitWithTimeoutNoTimeout` | covered | Native receive/select timer scenarios check virtual time and context cancellation; Await helpers are replaced by native synchronization. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestRecursiveEagerCoroutine` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestEagerCoroutineWhileNotRunning` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestAwaitWithTimeoutCancellation` | covered | Native receive/select timer scenarios check virtual time and context cancellation; Await helpers are replaced by native synchronization. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestFutureSetValue` | obsolete | Mutable Settable and Chain futures are removed. Applications compose native Go channels; activity future result delivery is tested separately. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestFutureFail` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamFutureFailure](workflow/upstream_future_test.go) [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestFutureSet` | obsolete | Mutable Settable and Chain futures are removed. Applications compose native Go channels; activity future result delivery is tested separately. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestFutureChain` | obsolete | Mutable Settable and Chain futures are removed. Applications compose native Go channels; activity future result delivery is tested separately. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSelectFuture` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestFutureToChannelWaitsAndSupportsIndependentTypedExtraction](workflow/activity_test.go) |
| `TestSelectDecodeFuture` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestFutureToChannelWaitsAndSupportsIndependentTypedExtraction](workflow/activity_test.go) |
| `TestDecodeFutureChain` | obsolete | Mutable Settable and Chain futures are removed. Applications compose native Go channels; activity future result delivery is tested separately. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestSelectFuture_WithBatchSets` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestFutureToChannelWaitsAndSupportsIndependentTypedExtraction](workflow/activity_test.go) |
| `TestChainedFuture` | obsolete | Mutable Settable and Chain futures are removed. Applications compose native Go channels; activity future result delivery is tested separately. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestFutureUnmarshalPointerToPointer` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamFutureUnmarshalPointerToPointer](workflow/upstream_future_test.go) |
| `TestContextCancelRace` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestContextChildCancelRace` | covered | Compiled native channel, select, recursive goroutine, or context-cancellation scenario runs at GOMAXPROCS 1/2/8; facade-specific scheduling order is replaced by native Go semantics. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestChildListAdd` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestChildListRemove` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestChildListRemoveRepeatedly` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestContextCancelOrderWithFlag` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestContextCancelOrderAfterRemoval` | obsolete | Removed cooperative dispatcher, eager-coroutine flag, and linked cancellation-list internals. Native lifecycle/cancellation behavior is tested in compiled workflows. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestDeadlockDetectorStackTrace` | obsolete | Removed cooperative coroutine detector internals; native workflow task deadlines and resource diagnostics have direct coverage. [TestResourcesCompiled](example/resources/check/main_test.go) |
| `TestChainAlreadyReadyFuture` | obsolete | Mutable Settable and Chain futures are removed. Applications compose native Go channels; activity future result delivery is tested separately. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestChainAlreadyReadyFuturePropagates` | obsolete | Mutable Settable and Chain futures are removed. Applications compose native Go channels; activity future result delivery is tested separately. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |

### internal/internal_deployment_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestDeploymentClientSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `deploymentClientTestSuite.TestSetCurrentDeployment` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `deploymentClientTestSuite.TestDeploymentIterator_NoError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `deploymentClientTestSuite.TestIteratorError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_eager_activity_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestEagerActivityDisabled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerActivityNoActivityWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerActivityWrongTaskQueue` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerActivityMaxPerTask` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerActivityCounts` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_eager_workflow_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestEagerWorkflowDispatchNoWorkerOnTaskQueue` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerWorkflowDispatchAvailableWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerWorkflowDispatchWithDeploymentOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerWorkflowDispatchWithoutDeploymentVersioning` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEagerWorkflowExecutor` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_event_handlers_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestDecodedValue` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecodedValueWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_DecodedValuePtr` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_DecodedValueNil` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ValidateAndSerializeSearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. The native boundary has supplemental coverage. [TestRawSearchAttributePayloadNotReserialized](workflow/metadata_test.go) |
| `TestPreferredVersionProviderOutOfRange` | retained | Host SDK provider bounds remain unchanged. Native preferred-version provider integration is separately blocked. |
| `Test_UpsertSearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_MergeSearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ValidateAndSerializeMemo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. The native boundary has supplemental coverage. [TestMemoEncodingPolicyAndFallback](workflow/metadata_test.go) |
| `Test_UpsertMemo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_MergeMemo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_GetChangeVersion` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_GetChangeVersions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_CreateSearchAttributesForChangeVersion` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestUpdateEvents` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestUpdateEventsPanic` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_flags_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLoadFlagOverridesFromEnv` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestOrderedChildCancelFlagDisabled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSet` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestTryUse` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_message_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestEventMessageIndex` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_nexus_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestExecuteNexusOperationHeaderAvailableToInterceptors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestExecuteNexusOperationNexusHeaderAvailableToInterceptors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusClientValidation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestResolveNexusOperationName` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_nexus_signal_link_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSignalForwardsRequestLinksAndCapturesResponseLink` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSignalAgainstOlderServerCapturesNoResponseLink` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMultipleSignalsAccumulateAllResponseLinks` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSignalWithStartForwardsRequestLinksAndCapturesResponseLink` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSignalWithStartAgainstOlderServerCapturesNoResponseLink` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMixedSignalAndSignalWithStartAccumulateAllResponseLinks` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSignalOutsideNexusContextCapturesNoResponseLink` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_nexus_task_handler_payload_serializer_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPayloadSerializerDeserializeErrors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadSerializerDeserializeSuccess` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_nexus_task_handler_response_link_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestAsyncResponseIncludesSignalResponseLinks` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSyncResponseIncludesSignalResponseLinks` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestResponseOmitsResponseLinksWhenNoneStashed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusStartRequestIDPropagatesToActivityStart` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMalformedRequestLinkLogsStructuredFields` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_schedule_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestScheduleClientSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `scheduleClientTestSuite.TestCreateScheduleClient` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `scheduleClientTestSuite.TestCreateScheduleNoID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `scheduleClientTestSuite.TestCreateScheduleWithMemoAndSearchAttr` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `scheduleClientTestSuite.TestScheduleIterator_NoError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `scheduleClientTestSuite.TestIteratorError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `scheduleClientTestSuite.TestCreateScheduleWorkflowMemoDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `scheduleClientTestSuite.TestCreateScheduleWorkflowMemoUserAndDefaultConverterFail` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `scheduleClientTestSuite.TestDescribeSchedulePopulatesPriority` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_search_attributes_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSearchAttributesKeywordList` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestSearchAttributesDeepCopy` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_task_handlers_interfaces_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPollLayerInterfacesTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `PollLayerInterfacesTestSuite.TestProcessWorkflowTaskInterface` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollLayerInterfacesTestSuite.TestProcessActivityTaskInterface` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollLayerInterfacesTestSuite.TestGetNextCommands` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollLayerInterfacesTestSuite.TestGetNextCommandsSdkFlags` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollLayerInterfacesTestSuite.TestMessageCommands` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollLayerInterfacesTestSuite.TestEmptyPages` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_task_handlers_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestTaskHandlersTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `TestActivityCancellationCallbacksCancel` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_WorkflowExecutionStarted` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_WorkflowExecutionStartedWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_BinaryChecksum` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestRespondsToWFTWithWorkerBinaryID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestStickyLegacyQueryTaskOnEvictedCache` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_ActivityTaskScheduled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_QueryWorkflow_Sticky` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_QueryWorkflow_NonSticky` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestCacheEvictionWhenErrorOccurs` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWithMissingHistoryEvents` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWithTruncatedHistory` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestSideEffectDefer` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestSideEffectDefer_NoCache` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_NondeterministicDetection` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_WorkflowReturnsPanicError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_WorkflowPanics` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestGetWorkflowInfo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. The native boundary has supplemental coverage. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `TaskHandlersTestSuite.TestConsistentQuery_InvalidQueryTask` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestConsistentQuery_Success` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_CancelActivityBeforeSent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_PageToken` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_DuplicateMessagesPanic` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_Messages` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_Message_Mixed_Types` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestWorkflowTask_Message_Admitted_Paged` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestLocalActivityRetry_Workflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestLocalActivityRetry_WorkflowTaskHeartbeatFail` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeartBeat_NoError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestHeartBeat_NilResponseWithError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestHeartBeat_NilResponseWithNamespaceNotActiveError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestActivityExecutionDeadline` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestActivityExecutionWorkerStop` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TaskHandlersTestSuite.TestActivityCancellationUsesIsCanceledError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_NonDeterministicCheck` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_IsCommandMatchEvent_UpsertWorkflowSearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_ProtocolCommandEventMatching` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_IsSearchAttributesMatched` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_IsCommandMatchEvent_ModifyWorkflowProperties` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_IsMemoMatched` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHeartbeatThrottleInterval` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestResetIfDestroyedTaskPrep` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHistoryIteratorMaxEventID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_task_pollers_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLocalActivityTaskPollerReturnsNilWhenTunnelStops` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLocalActivityTunnelTaskQueueGrowsWithDemand` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLocalActivityTunnelTaskQueuePreservesOrder` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPollRequestsIncludeWorkerControlTaskQueue` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWFTRacePrevention` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWFTCorruption` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWFTReset` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWFTPanicInTaskHandler` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLegacyQueryProcessingFailureReportedAsQueryFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLegacyQueryOutboundVisitorFailureReportedAsQueryFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLegacyQueryInboundVisitorFailureReportedAsQueryFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestErrorToFailWorkflowTaskCause` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityTaskProcessedAfterStopForShutdownDrain` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityTaskNotProcessedAfterStopForLegacyShutdown` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowTaskProcessAfterStopMatchesShutdownMode` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDoPollGracefulShutdown` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowTaskStorageMetricsTotalDuration` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_update_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestUpdateHandlerPanicHandling` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestUpdateHandlerFnValidation` | covered | All upstream update handler signature cases use context.Context. [TestUpstreamUpdateHandlerFnValidation](workflow/upstream_handlers_test.go) |
| `TestUpdateValidatorFnValidation` | covered | All upstream validator signature cases and matching native arguments are checked. [TestUpstreamUpdateValidatorFnValidation](workflow/upstream_handlers_test.go) |
| `TestDefaultUpdateHandler` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestInvalidUpdateStateTransitions` | retained | Update protocol states and predicates remain host SDK implementations. Native handler acceptance/completion is checked separately. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestCompletedEventPredicate` | retained | Update protocol states and predicates remain host SDK implementations. Native handler acceptance/completion is checked separately. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestAcceptedEventPredicate` | retained | Update protocol states and predicates remain host SDK implementations. Native handler acceptance/completion is checked separately. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |

### internal/internal_utils_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestChannelBuilderOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewValues` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewValue` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestFailureToError_ApplicationError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestFailureToError_CancelError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestErrorToFailure_TimeoutError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestFailureToError_TimeoutError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_versioning_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_DetectEnhancedNotSupported_fromProtoResponse` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_TaskQueueDescription_fromProtoResponse` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_worker_base_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPollerAutoscalerSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `ScalableTaskPollerSuite.TestNewScalableTaskPollerSetsTaskPollerType` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestNewScalableTaskPollerUsesDynamicRunnerOnlyForAutoscaling` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestSlotReservationDataUsesKnownTaskQueueKind` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestTrackingSlotSupplierPassesTaskQueueKind` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestInitializeTaskPollersRequiresBalancer` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestTrackingSlotSupplierPublishesLatestConcurrentState` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestScalableTaskPollerSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `PollerAutoscalerSuite.TestErrorScaleDown` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollerAutoscalerSuite.TestScaleDownOnEmptyTask` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollerAutoscalerSuite.TestScaleUpOnDelay` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestAutoscalingConcurrencyScalesUpToMaximum` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestAutoscalingScalesDownToMinimum` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestAutoscalingDoesNotHoldSlotWhileWaitingForPollCapacity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestUnknownCapacityBalancerWaitsBeforePollCapacity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestWorkflowAutoscalingBalancerPreservesBothQueueKinds` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestTaskNotDroppedDuringShutdown` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestAutoscalingTaskNotDroppedDuringShutdown` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestTaskDrainDuringShutdownRespectsDispatchRate` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestTaskDrainAfterDispatchLimiterCancelReleasesUnprocessedTasks` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestTaskNotProcessedDuringLegacyShutdown` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStopTimeoutBoundsPollerDrain` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLegacyStopReturnsPromptlyWithBlockedPoller` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollerAutoscalerSuite.TestAutoscaleDownOnTimeoutWithCapability` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollerAutoscalerSuite.TestAutoscaleDownOnTimeoutWithoutCapability` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollerAutoscalerSuite.TestAutoscaleDownOnTimeoutClampsToMin` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `PollerAutoscalerSuite.TestErrorScaleDownWithCapability` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerPreservesQueueKinds` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerPrefersStickyBacklog` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerAllowsNormalAtStickyTarget` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestBalancerTargetWakesNormal` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerUnknownMaximum` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerUnknownMaximumMatchesGenericFairness` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestUnknownCapacityPrefersSticky` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerUnknownMaximumCancellation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerCancellation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerRejectsInvalidKind` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerIgnoresInvalidKind` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerStickyStartWakesNormal` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowAutoscalingBalancerIgnoresUnchangedBacklog` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestWorkflowAutoscalingBalancerConfiguration` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestConfigurePollersRejectsInconsistentBalancer` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `ScalableTaskPollerSuite.TestNewScalableTaskPollerAllTypes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStopReturnsPromptlyWithPollerAwaitingSessionToken` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_worker_cache_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWorkerCacheTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `WorkerCacheSuite.TestCreateAndFree` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_worker_deployment_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWorkerDeploymentClientSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `workerDeploymentClientTestSuite.TestWorkerDeploymentIterator_NoError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workerDeploymentClientTestSuite.TestWorkerDeploymentIteratorError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workerDeploymentClientTestSuite.TestWorkerDeploymenNilTimestamp` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_worker_environment_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestDetectEnvironmentInfoReportsGoRuntimeAndPlatform` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDetectHostingEnvironments` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCgroupsIndicateDocker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDetectRuntimesRoadRunner` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_worker_heartbeat_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestStopCancelsInFlightHeartbeatRPC` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerCommandPollUsesWorkerCommandsQueue` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerCommandCancelActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerCommandsDisabledDoesNotPoll` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerHeartbeatSendsImmediatelyWithIdentity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerHeartbeatElapsedSinceLastHeartbeatUnsetOnInitialHeartbeat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerHeartbeatEnvironmentSentUntilAccepted` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerHeartbeatEnvironmentDisabled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerHeartbeatEnvironmentIncludedInShutdownHeartbeat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_worker_interfaces_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestInterfacesTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `InterfacesTestSuite.TestInterface` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_worker_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestInternalWorkerTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_IncompleteWorkflowExecution` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_LocalActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_GetVersion` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_LocalAndRemoteActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_GetVersion_ReplacedChangeID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_GetVersionRemoved` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_GetVersion_AddNewBefore` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_GetVersionWithSideEffect` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_CancelActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_CancelActivity_Unusual_Ordering` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_CancelTimer` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowCancelTimerAfterActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_FailedToStartChildWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_CancelChildWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_ChildWorkflowCancellation_WhenWorkflowCanceled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_ChildWorkflowCancellation_Unusual_Ordering` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_CancelWorkflowWhileSleepingWithActivities` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_LocalActivity_Activity_Type_Mismatch` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_SideEffect` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistory_ReplayRunID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistoryFromFileParent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestReplayWorkflowHistoryFromFile` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestWorkflowTaskHandlerWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCreateWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCreateWorkerWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestPollerAutoscalingAutoEnrollWithDefaults` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestPollerAutoscalingAutoEnrollDisabled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestPollerAutoscalingAutoEnrollRespectsExplicitConfig` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestPollerAutoscalingAutoEnrollSessionWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestSlotSupplierReturnsErrorCanContinue` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCreateWorkerRun` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestNoActivitiesOrWorkflows` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCleanupIsBestEffort` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestStartWorkerAfterStopped` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityWithContextAwareDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityById` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityByIDWithContextAwareDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityByIDWithOptions_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityByID_DelegatesToWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestRecordActivityHeartbeatByIDWithOptions_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityWithOptions_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivity_DelegatesToWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityByActivityIDWithOptions_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestCompleteActivityByActivityID_DelegatesToWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestRecordActivityHeartbeatWithOptions_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestRecordActivityHeartbeat_DelegatesToWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestRecordActivityHeartbeat` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestRecordActivityHeartbeatWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestRecordActivityHeartbeatByID` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRegisterStructWithInvalidFnsWithoutSkipFails` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRegisterStructWithInvalidWorkflowContextFnFails` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRegisterStructWithInvalidActivityWithWorkflowContextFails` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestVariousActivitySchedulingOption` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRegisterVariousWorkflowTypes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityErrorWithDetailsWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityCanceledErrorWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityExecutionVariousTypesWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestActivityNilArgs` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. The native boundary has supplemental coverage. [TestTypedActivityArgumentsAndResults](workflow/workflow_test.go) |
| `TestWorkerOptionInvalid` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerOptionDefaults` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerOptionNonDefaults` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLocalActivityWorkerOnly` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestIsNonRetriableError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerRegisterDisabledWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkerBuildIDAndSessionPanic` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestSessionWorkerShutdownSendsShutdownWorkerForSessionTaskQueues` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestSessionWorkerShutdownSetsNoRepollOnSessionWorkers` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestSessionWorkerShutdownDrainModeMatchesAggregateWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestHistoryFromJSON` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestFunctionLiteralDuplicateRegistrationHint` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestAliasStringNameClash` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. The native boundary has supplemental coverage. [TestOnlyFunctionReferencesResolveActivityAliases](temporalbridge/bridge_test.go) |
| `TestAliasUnqualifiedNameClash` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. The native boundary has supplemental coverage. [TestWorkflowAliasesTrackOrdinaryRegistrations](worker/worker_test.go) |
| `internalWorkerTestSuite.TestReservedTemporalName` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `internalWorkerTestSuite.TestRegisterMultipleDynamicWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_workers_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWorkersTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `WorkersTestSuite.TestWorkflowWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestWorkflowWorkerSlotSupplier` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestActivityWorkerSlotSupplier` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestErrorProneSlotSupplier` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestActivityWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestActivityWorkerStop` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestPollWorkflowTaskQueue_InternalServiceError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestLongRunningWorkflowTask` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestMultipleLocalActivities` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestWorkerMultipleStop` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `WorkersTestSuite.TestWorkerTaskQueueLimitDisableEager` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_workflow_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestHistoryEventIteratorSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `historyEventIteratorSuite.TestIterator_NoError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `historyEventIteratorSuite.TestIterator_NoError_EmptyPage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `historyEventIteratorSuite.TestIterator_NoError_EmptyPageNoHasHasNext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `historyEventIteratorSuite.TestIteratorError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowRunSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `workflowRunSuite.TestCancelWorkflowWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestCancelWorkflowLegacyMethod` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestTerminateWorkflowWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestTerminateWorkflowLegacyMethod` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoDup_Success` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoDup_RawHistory_Success` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_StartWorkflowResponseInfo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWorkflowExecutionAlreadyStartedError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWorkflowExecutionAlreadyStartedErrorAllowStarted` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWorkflowExecutionAlreadyStartedError_RawHistory` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoIdInOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoIdInOptions_RawHistory` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoDup_Canceled` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoDup_Failed` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoDup_Terminated` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoDup_TimedOut` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflow_NoDup_ContinueAsNew` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestGetWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestGetWorkflowNoRunId` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestGetWorkflowNoExtantWorkflowAndNoRunId` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_Retry` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_DefaultTimeout` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_OperationNotExecuted` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_Abort` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_Errors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_ServerResponseCountMismatch` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_ServerStartResponseTypeMismatch` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowRunSuite.TestExecuteWorkflowWithUpdate_ServerUpdateResponseTypeMismatch` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWorkflowClientSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `TestLoadCapabilitiesUnknownMethodUnimplementedUsesEmptyCapabilities` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLoadCapabilitiesNonUnknownMethodUnimplementedFails` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSignalWithStartWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSignalWithStartWorkflowWithContextAwareDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestUpdateWorkflowWithContextAwareDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSignalWithStartWorkflowValidation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestStartWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestEagerStartWorkflowNotSupported` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestEagerStartWorkflowNoWorker` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestEagerStartWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestEagerStartWorkflowStartRequestFail` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestExecuteWorkflowWithDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestExecuteWorkflowWithContextAwareDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestStartWorkflowWithMemoAndSearchAttr` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSignalWithStartWorkflowWithMemoAndSearchAttr` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestStartWorkflowWithVersioningOverride` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestStartWorkflowWithOneTimeVersioningOverride` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSignalWithStartWorkflowWithVersioningOverride` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSignalWithStartWorkflowWithOneTimeVersioningOverride` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestGetWorkflowMemo` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestStartWorkflowWithMemoDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestStartWorkflowWithMemoUserAndDefaultConverterFail` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSerializeSearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestSerializeSearchAttributesOmitsNilValuesOnStart` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestListWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestListArchivedWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestScanWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestCountWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestGetSearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientCloseCount` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCompletedUpdateHandle` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestUpdate` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPollActivityResult` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `workflowClientTestSuite.TestPollActivityResultUsesPerIterationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_workflow_execution_options_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_WorkflowExecutionOptions_fromProtoResponse` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_WorkflowExecutionOptions_toProto_OneTimeVersioningOverride` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_WorkflowExecutionOptionsChanges_toProto_OneTimeVersioningOverride` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_UpdateWorkflowExecutionOptionsRequest_validateAndConvertToProto_OneTimeVersioningOverride` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_workflow_task_pagination_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPaginateWorkflowTaskCompletion_FitsInSinglePage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPaginateWorkflowTaskCompletion_SplitsCommandsAcrossPages` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPaginateWorkflowTaskCompletion_SingleCommandLargerThanPageIsNotSplit` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPaginateWorkflowTaskCompletion_NoCommandsIsNotSplit` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/internal_workflow_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWorkflowUnitTest` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `WorkflowUnitTest.Test_WorldWorkflow` | covered | Compiled workflow completion and returned PanicError type/message/stack preservation through the new SDK. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_WorkflowWithLocalActivityDefaultRetryPolicy` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowUnitTest.Test_WorkflowWithLocalActivityWithMaxAttempts` | retained | In-task local retry counts and MaximumAttempts are enforced by the host SDK local-activity worker. Native option forwarding and durable backoff have separate boundary coverage. [TestLocalTypesOptionsBackoffAndCancel](temporalbridge/api_coverage_test.go) [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowUnitTest.Test_WorkflowWithLocalActivityWithMaxAttemptsExceeded` | retained | In-task local retry counts and MaximumAttempts are enforced by the host SDK local-activity worker. Native option forwarding and durable backoff have separate boundary coverage. [TestLocalTypesOptionsBackoffAndCancel](temporalbridge/api_coverage_test.go) [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowUnitTest.Test_SingleActivityWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_SplitJoinActivityWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowPanic` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `TestWorkflowReturnsPanic` | covered | Compiled workflow completion and returned PanicError type/message/stack preservation through the new SDK. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_ClockWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestTimerWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestActivityCancellation` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestActivityCancellationUsesSDKCompletionPolicy](temporalbridge/activity_test.go) [TestCancelActivityRepliesOnceAndIgnoresLateCallback](temporalbridge/bridge_test.go) |
| `WorkflowUnitTest.Test_ExternalExampleWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowUnitTest.Test_ContinueAsNewWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledContinuationAndSavedVersionHistory](example/continuation/check/main_test.go) |
| `WorkflowUnitTest.Test_CancelWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) [TestResourcesCompiled](example/resources/check/main_test.go) |
| `WorkflowUnitTest.Test_CancelWorkflowAfterActivity` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) [TestResourcesCompiled](example/resources/check/main_test.go) |
| `WorkflowUnitTest.Test_CancelledWorkflowCantStartChild` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestInvalidChildResolvesBothFuturesWithoutScheduling](workflow/child_test.go) [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowUnitTest.Test_SignalWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledProactiveSignalDelivery](example/signals/check/main_test.go) |
| `WorkflowUnitTest.Test_CorruptedSignalWorkflow_ShouldLogMetricsAndNotPanic` | covered | `malformed_signals`: Compiled native workflow port verifies corrupt values are discarded, error details are logged, the corrupted-signal counter increments once per discarded signal, and valid values remain readable at GOMAXPROCS 1/2/8 with configured converters and interceptor adapters. [TestUpstreamCorruptedSignalReceive](example/signals/check/corrupt_test.go) |
| `WorkflowUnitTest.Test_CorruptedSignalWorkflow_OnSelectorRead_ShouldLogMetricsAndNotPanic` | covered | `malformed_signals`: Compiled native workflow port verifies corrupt values are discarded, error details are logged, the corrupted-signal counter increments once per discarded signal, and valid values remain readable at GOMAXPROCS 1/2/8 with configured converters and interceptor adapters. [TestUpstreamCorruptedSignalSelect](example/signals/check/corrupt_test.go) |
| `WorkflowUnitTest.Test_CorruptedSignalWorkflow_ReceiveAsync_ShouldLogMetricsAndNotPanic` | covered | `malformed_signals`: Compiled native workflow port verifies corrupt values are discarded, error details are logged, the corrupted-signal counter increments once per discarded signal, and valid values remain readable at GOMAXPROCS 1/2/8 with configured converters and interceptor adapters. [TestUpstreamCorruptedSignalReceiveAsync](example/signals/check/corrupt_test.go) |
| `WorkflowUnitTest.Test_CorruptedSignalOnClosedChannelWorkflow_ReceiveAsync_ShouldComplete` | covered | `malformed_signals`: Compiled native workflow port verifies corrupt values are discarded, error details are logged, the corrupted-signal counter increments once per discarded signal, and valid values remain readable at GOMAXPROCS 1/2/8 with configured converters and interceptor adapters. A closed raw interceptor adapter supplies end-of-stream because SDK signal channels are receive-only in the native API. [TestUpstreamCorruptedSignalClosedReceiveAsync](example/signals/check/corrupt_test.go) |
| `WorkflowUnitTest.Test_CorruptedSignalOnClosedChannelWorkflow_Receive_ShouldComplete` | covered | `malformed_signals`: Compiled native workflow port verifies corrupt values are discarded, error details are logged, the corrupted-signal counter increments once per discarded signal, and valid values remain readable at GOMAXPROCS 1/2/8 with configured converters and interceptor adapters. A closed raw interceptor adapter supplies end-of-stream because SDK signal channels are receive-only in the native API. [TestUpstreamCorruptedSignalClosedReceive](example/signals/check/corrupt_test.go) |
| `WorkflowUnitTest.Test_CloseChannelWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_CloseChannelInSelectWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_SelectDoesntLoopForever` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_BufferedChanWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_BufferedChanWithSelectorWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_ActivityOptionsWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamGetActivityOptions](workflow/upstream_options_test.go) [TestActivityOptionsReachSDKUnchanged](temporalbridge/activity_test.go) |
| `WorkflowUnitTest.Test_MemoWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `WorkflowUnitTest.Test_waitGroupNegativeCounterPanicsWorkflowTest` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_WaitGroupMultipleConcurrentWaitsPanicsWorkflowTest` | obsolete | Native sync.WaitGroup supports multiple concurrent Wait calls; expecting the old facade panic is obsolete. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_WaitGroupMultipleWaitsWorkflowTest` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_WaitGroupWaitForMWorkflowTest` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_WaitGroupWorkflowTest` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowUnitTest.Test_MutatingFunctionsInSideEffect` | obsolete | SideEffect and MutableSideEffect are explicitly excluded by API_COVERAGE_PLAN.md; nondeterministic work belongs in host/local activities. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowUnitTest.Test_MutatingFunctionsInMutableSideEffect` | obsolete | SideEffect and MutableSideEffect are explicitly excluded by API_COVERAGE_PLAN.md; nondeterministic work belongs in host/local activities. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowUnitTest.Test_MutatingFunctionsInQueries` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledQueriesAndCompletedState](example/query/check/main_test.go) |
| `WorkflowUnitTest.Test_MutatingFunctionsInUpdateValidator` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `WorkflowUnitTest.Test_StaleGoroutinesAreShutDown` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) [TestResourcesCompiled](example/resources/check/main_test.go) |
| `TestQueryFnValidation` | covered | All upstream query signature cases, translated to native handler contracts. [TestUpstreamQueryFnValidation](workflow/upstream_handlers_test.go) |
| `TestParamValidator` | covered | Upstream equivalent-argument cases translated to native update handler/validator signatures. [TestUpstreamEquivalentParams](workflow/upstream_handlers_test.go) |
| `TestStackTraceInvalidDepthBounded` | covered | `stack_trace_query`: Native counterpart exercises shallow/deep values 2 and 100 through __stack_trace, checking readable blocking/entry frames and bounded deep formatting. The private coroutine frame-omission parameter and exact three-line trimming are replaced by native full stacks with explicit Go frame elision. [TestStackTraceInvalidDepthBounded](example/query/check/main_test.go) [TestCompiledQueriesAndCompletedState](example/query/check/main_test.go) |

### internal/internal_workflow_testsuite_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestUnitTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `WorkflowTestSuiteUnitTest.Test_ActivityMockFunction` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityMockFunctionZero` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowReturnedCancel` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityByNameMockFunction` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityMockFunctionWithDataConverter` | covered | `context_propagation`: Native per-context serializers send custom/default formats to host activity mocks and preserve the scheduling converter when waiting with another context. Local activities, children, query/update registration and direct overrides without a worker factory are also covered. [TestUpstreamNativeContextPropagation](example/contextpropagation/check/main_test.go) [TestPerContextFutureRetainsScheduledConverter](workflow/context_converter_test.go) [TestPerContextCustomEncodingAndContinuation](workflow/context_converter_test.go) [TestNativeContextHistoryReplay](example/contextpropagation/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityMockValues` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_OnActivityStartedListener` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_TimerWorkflow_ClockAutoFastForward` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowAutoForwardClock` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowMixedClock` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowActivityCancellation` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityWithUserContext` | retained | This test exercises the unchanged host activity-only test environment or a host environment field setter; no native workflow executes. |
| `WorkflowTestSuiteUnitTest.Test_ActivityWithHeaderContext` | retained | This test exercises the unchanged host activity-only test environment or a host environment field setter; no native workflow executes. |
| `WorkflowTestSuiteUnitTest.Test_CompleteActivity` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityReturnsErrActivityResultPending` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityHeartbeatMock` | retained | This test exercises the unchanged host activity-only test environment or a host environment field setter; no native workflow executes. |
| `WorkflowTestSuiteUnitTest.Test_WorkflowHeartbeatMock` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowCancellation` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkerLifecycleAndSDKReplay](example/lifecycle/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflowCancellation` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SideEffect` | obsolete | SideEffect/MutableSideEffect deliberately excluded; host or local activities own nondeterministic work. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_OnSideEffectAndActivityMock` | obsolete | SideEffect/MutableSideEffect deliberately excluded; host or local activities own nondeterministic work. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_OnMutableSideEffect` | obsolete | SideEffect/MutableSideEffect deliberately excluded; host or local activities own nondeterministic work. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MutableSideEffect_HonorsEqualsFunc` | obsolete | SideEffect/MutableSideEffect deliberately excluded; host or local activities own nondeterministic work. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_LongRunningSideEffect` | obsolete | SideEffect/MutableSideEffect deliberately excluded; host or local activities own nondeterministic work. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SideEffect_WithVersion` | obsolete | SideEffect/MutableSideEffect deliberately excluded; host or local activities own nondeterministic work. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_Basic` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_ContinueAsNew` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_BasicWithDataConverter` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflowCancel` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_Mock` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_Mock_Panic_GetChildWorkflowExecution` | covered | `test_environment`: Ports invalid mock return handling, child-start progress and the workflow/child/panic error chain. [TestChildWorkflowMockPanicGetExecution](testsuite/upstream_test.go) |
| `WorkflowTestSuiteUnitTest.Test_NestedChildWorkflow_RootWorkflowExecution` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_StartFailed` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_Listener` | covered | `test_environment`: Ports child-start metadata, child-completion result and parent result assertions with host context.Context listeners. [TestChildWorkflowListener](testsuite/upstream_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_Clock` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_Abandon` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_RequestCancel` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflow_Terminated` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockActivityWait` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockActivityWaitFn` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockWorkflowWait` | covered | `test_environment`: Ports immediate, ten-minute and two-hour child mock delays against a one-hour parent timer. [TestMockWorkflowWait](testsuite/upstream_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockPanic` | retained | `test_environment`: The host SDK retains the original activity-mock panic assertions. The isolate facade wraps its mock controls; linked tests additionally check native workflow callback and Run panics without claiming to duplicate this activity case. [TestMockPanicAndUnexpectedCallsBecomeWorkflowFailures](testsuite/native_test.go) [TestMockPanicThroughRun](testsuite/upstream_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockPanicThoughRun` | retained | `test_environment`: The host SDK retains the original activity-mock panic assertions. The isolate facade wraps its mock controls; linked tests additionally check native workflow callback and Run panics without claiming to duplicate this activity case. [TestMockPanicAndUnexpectedCallsBecomeWorkflowFailures](testsuite/native_test.go) [TestMockPanicThroughRun](testsuite/upstream_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWithChild` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_GetVersion` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledContinuationAndSavedVersionHistory](example/continuation/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_PreferredVersionProvider` | covered | Compiled native workflow uses the real host environment and delegated worker.Options.PreferredVersionProvider; verifies default selection, repeated-call caching, single provider invocation and exact change/version bounds. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockGetVersion` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_UpsertSearchAttributes_ReservedKey` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockUpsertSearchAttributes` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockUpsertTypedSearchAttributes` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockUpsertMemo` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityWithPointerTypes` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamFutureUnmarshalPointerToPointer](workflow/upstream_future_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityWithProtoPayload` | retained | The source test uses the unchanged host activity test environment. Retained here, with native workflow/activity counterparts and compiled ownership, codec and replay coverage. [TestUpstreamActivityWithProtoPayload](example/protobufvalues/check/upstream_test.go) [TestGeneratedProtobufWorkflowAPIs](example/protobufvalues/check/main_test.go) [TestGeneratedProtobufHistoryReplay](example/protobufvalues/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityWithRandomProto` | retained | The source test uses the unchanged host activity test environment. Retained here, with native workflow/activity counterparts and compiled ownership, codec and replay coverage. [TestUpstreamActivityWithRandomProto](example/protobufvalues/check/upstream_test.go) [TestGeneratedProtobufWorkflowAPIs](example/protobufvalues/check/main_test.go) [TestGeneratedProtobufHistoryReplay](example/protobufvalues/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityRegistration` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestActivityAliasesAndOrdinaryActivities](worker/worker_test.go) [TestOnlyFunctionReferencesResolveActivityAliases](temporalbridge/bridge_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowRegistration` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkflowAliasesTrackOrdinaryRegistrations](worker/worker_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityFriendlyName` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestActivityAliasesAndOrdinaryActivities](worker/worker_test.go) [TestOnlyFunctionReferencesResolveActivityAliases](temporalbridge/bridge_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowFriendlyName` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestWorkflowAliasesTrackOrdinaryRegistrations](worker/worker_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowHeaderContext` | covered | `context_propagation`: Marked native propagators extract initial headers and inject inherited headers into children and host activities; compiled assertions match upstream context values. [TestUpstreamNativeContextPropagation](example/contextpropagation/check/main_test.go) [TestNativeContextHistoryReplay](example/contextpropagation/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflowContextPropagation` | covered | `context_propagation`: A derived native context overrides child headers while preserving the parent value. Both compiled host-test environments and saved live histories verify extraction in the child. [TestUpstreamNativeContextPropagation](example/contextpropagation/check/main_test.go) [TestNativeContextHistoryReplay](example/contextpropagation/check/main_test.go) [TestContextPropagatorOrderIsolationAndErrors](workflow/context_converter_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityFullyQualifiedName` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestActivityAliasesAndOrdinaryActivities](worker/worker_test.go) [TestOnlyFunctionReferencesResolveActivityAliases](temporalbridge/bridge_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowUnknownName` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_QueryWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledQueriesAndCompletedState](example/query/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_QueryWorkflow_NilArgsAndResult` | covered | `test_environment`: Ports nil query input/result after actual workflow completion with retained native state. [TestQueryNilArgsAndResult](testsuite/upstream_test.go) |
| `WorkflowTestSuiteUnitTest.Test_QueryChildWorkflow` | covered | `test_environment`: Ports child queries by workflow ID while waiting for a signal, waiting for an activity, and after completion. [TestQueryChildWorkflow](testsuite/upstream_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowWithLocalActivity` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowWithLocalActivityNextDelay` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_LocalActivity` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowLocalActivityWithMockAndListeners` | covered | Compiled native workflow runs through the existing host TestWorkflowEnvironment factory path; ports the original activity mock/listener or virtual-clock assertions. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_LocalActivityWithHeaderContext` | retained | `context_propagation`: The source test exclusively exercises the unchanged host TestActivityEnvironment and its local activity header extraction. Supplemental native workflow tests now inject the same header into real host local activities. [TestUpstreamNativeContextPropagation](example/contextpropagation/check/main_test.go) [TestNativeContextHistoryReplay](example/contextpropagation/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SignalChildWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SignalExternalWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_CancelChildWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_CancelExternalWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_DisconnectedContext` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowIDReusePolicy` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_Channel` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ContextMisuse` | obsolete | The old per-coroutine context restriction is removed: native context.Context is shared safely between goroutines. Native timer/context concurrency replaces this error expectation. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_DrainSignalChannel` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledProactiveSignalDelivery](example/signals/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityRetry` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityRetry_DefaultRetry` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityRetry_NoInitialInterval` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityRetry_NoRetries` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityHeartbeatRetry` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_LocalActivityRetry` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_LocalActivityRetry_MaxAttempts_Respected` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_LocalActivityRetryOnCancel` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityRetryOnCancel` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflowRetry` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflowTypedSearchAttributes` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SignalChildWorkflowRetry` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_TestWorkflowTimeoutInBusyLoop` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_TestChildWorkflowTimeout` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SameActivityIDFromDifferentChildWorkflow` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_MockChildWorkflowAlreadyRunning` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflowAlreadyRunning` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_CronWorkflow` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_CronHasLastResult` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_CronGetLastFailure` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityWithProgress` | retained | This test exercises the unchanged host activity-only test environment or a host environment field setter; no native workflow executes. |
| `WorkflowTestSuiteUnitTest.Test_ActivityHeartbeat` | retained | This test exercises the unchanged host activity-only test environment or a host environment field setter; no native workflow executes. |
| `WorkflowTestSuiteUnitTest.Test_ActivityGoexit` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SetWorkerStopChannel` | retained | This test exercises the unchanged host activity-only test environment or a host environment field setter; no native workflow executes. |
| `WorkflowTestSuiteUnitTest.Test_SetContinuedExecutionRunID` | retained | This test exercises the unchanged host activity-only test environment or a host environment field setter; no native workflow executes. |
| `WorkflowTestSuiteUnitTest.Test_ActivityTimeoutWithDetails` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityDeadlineExceeded` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityHeartbeatTimeout` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityHeartbeatTimeout_WithHeartbeat` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityHeartbeatTimeout_WithDetails` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityStartToCloseTimeout` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityStartToCloseTimeout_Respects` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityStartToCloseTimeout_HeartbeatPriority` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityStartToCloseTimeout_GracePeriod` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityStartToCloseTimeout_GracePeriodExceeded` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_AwaitWithTimeoutTimeout` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_AwaitWithTimeoutConditionMet_WithFlag` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_AwaitWithTimeoutConditionMet_WithoutFlag` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_NoDetachedChildWait` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledChildWorkflows](example/child/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ChildWorkflowActivityInfo` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityHeartbeatCancel` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_ActivityOnCancelledContext` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestExecuteActivityValidatesReferencesAndArguments](workflow/activity_test.go) |
| `WorkflowTestSuiteUnitTest.Test_WorkflowGetCurrentHistoryLength` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SameWorkflowAndActivityNames` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SignalNotLost` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledProactiveSignalDelivery](example/signals/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_SignalLost` | obsolete | Test documents discarded unread signals when workflow exits without draining. Proactive native admission/drain is explicitly checked in the new SDK. [TestCompiledProactiveSignalDelivery](example/signals/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.TestChannelWorkerPattern` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `WorkflowTestSuiteUnitTest.Test_OnWorkflowMockSeesHeaderContext` | covered | `context_propagation`: A native parent injects its header when scheduling an ordinary SDK child; the host workflow mock matcher asserts the extracted context value and mock expectations. [TestUpstreamNativeContextPropagation](example/contextpropagation/check/main_test.go) |

### internal/log/logger_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestMemoryLogger_With` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMemoryLoggerWithoutWith_With` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDefaultLogger_With` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestReplayLogger` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestReplayLogger_With` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMemoryLogger_WithSharesLock` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMemoryLogger_ConcurrentWithAndLines` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestReplayLogger_Skip` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/nexus_operations_cancellation_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNexusAbandonCancellationClearsCallbacks` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/nexus_serialization_context_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNexusSerializationContextInputAndResultIsolation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusTaskHandlerSerializationContextInputAndSyncResult` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusTaskHandlerFailureSerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusSerializationContextFailureConverterInTestEnvironment` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStandaloneNexusSerializationContextInputAndResult` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStandaloneNexusSerializationContextFailure` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStandaloneNexusSerializationContextUseExisting` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStandaloneNexusSerializationContextDescribeFailures` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusOperationFailureEventsUseScheduledFailureConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/payload_limits_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPayloadLimitOptionsToLimits` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadLimitsVisitorWarning` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadLimitsVisitorError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadLimitsVisitorAggregation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadLimitsVisitorErrorBeforeWarning` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadLimitsVisitorSpecializations` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadLimitsVisitorQueryResultExternalStorage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPayloadLimitsVisitorQueryResultConcurrentVisit` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCreateScheduleRequestSpecialization` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMemoLimitsVisitorWarning` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMemoLimitsVisitorError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/payload_visitor_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestVisitProtoPayloads_ConcurrentVisitors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestVisitProtoPayloads_SequentialVisitors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestCompositePayloadVisitor` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/protocol/registry_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestLookup` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClearCompleted` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/protocol/util_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNameFromMessage` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMustMarshalAny` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/resource_tuner_cancel_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestReserveSlotRejectsAlreadyCancelledContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestReserveSlotHonorsContextCancellation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/resource_tuner_eager_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestTryReserveSlotDoesNotBlockDuringRampWait` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRampThrottleBoundsIssuanceAcrossConcurrentReserveSlot` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRampThrottleStillBoundsIssuance` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/resource_tuner_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestPidDecisions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestPidDecisionEmitsUsageMetrics` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/serialization_context_client_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestClientStartWorkflow_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientSignalWorkflow_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientQueryWorkflow_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientTerminateWorkflow_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientDescribeWorkflow_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientPollWorkflowUpdate_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientUpdateWithStartWorkflow_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientStartActivity_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientDescribeActivity_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestClientActivityGet_SerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/serialization_context_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSerializationContextSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `SerializationContextTestSuite.TestActivityRoundTrip_SigningCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestActivityRoundTrip_CapturingDC` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestLocalActivityRoundTrip_CapturingDC` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestLocalActivityResult_EncodedWithLocalActivityContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestWorkflowInput_RequiresContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestChildWorkflowRoundTrip_SigningCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestSideEffect_SigningCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestEncryptionCodec_ConcurrentOperations` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestSignalExternalWorkflow_CapturingDC` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestQueryResult_CapturingDC` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestQueryRoundTrip_SigningCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestUpdateResult_CapturingDC` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestUpdateRoundTrip_SigningCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestActivityEnvironmentLocalActivity_UsesConfiguredDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestLocalActivityMock_SigningCodec` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestLocalActivity_OneShotDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestActivity_OneShotDataConverter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `SerializationContextTestSuite.TestLocalActivity_WorkflowBindingDropsSerializationContext` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/session_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSessionTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `SessionTestSuite.TestCreationCompletion` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestCreationWithOpenSessionContext` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamCreationWithOpenSessionContext](workflow/upstream_session_test.go) |
| `SessionTestSuite.TestCreationWithClosedSessionContext` | covered | Compiled session-from-closed/session-from-failed/session-activity-closed/session-completion-failure/session-timer cases assert fresh metadata, ordinary queue routing, closure after failure, and virtual time. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestCreationWithFailedSessionContext` | covered | Compiled session-from-closed/session-from-failed/session-activity-closed/session-completion-failure/session-timer cases assert fresh metadata, ordinary queue routing, closure after failure, and virtual time. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestCompletionWithClosedSessionContext` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamCompletionWithClosedOrFailedSessionContext](workflow/upstream_session_test.go) |
| `SessionTestSuite.TestCompletionWithFailedSessionContext` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamCompletionWithClosedOrFailedSessionContext](workflow/upstream_session_test.go) |
| `SessionTestSuite.TestGetSessionInfo` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamGetSessionInfo](workflow/upstream_session_test.go) |
| `SessionTestSuite.TestRecreation` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestMaxConcurrentSession_CreationOnly` | retained | Host SDK session-worker capacity and remote activity retry state machines remain unchanged. The bridge separately checks session queue routing. [TestSessionHostMetadata](temporalbridge/api_coverage_test.go) |
| `SessionTestSuite.TestMaxConcurrentSession_WithRecreation` | retained | Host SDK session-worker capacity and remote activity retry state machines remain unchanged. The bridge separately checks session queue routing. [TestSessionHostMetadata](temporalbridge/api_coverage_test.go) |
| `SessionTestSuite.TestSessionTaskQueue` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestSessionRecreationTaskQueue` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestExecuteActivityInFailedSession` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestExecuteActivityInClosedSession` | covered | Compiled session-from-closed/session-from-failed/session-activity-closed/session-completion-failure/session-timer cases assert fresh metadata, ordinary queue routing, closure after failure, and virtual time. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestSessionRecreateToken` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamSessionRecreateToken](workflow/upstream_session_test.go) |
| `SessionTestSuite.TestInvalidRecreateToken` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamInvalidRecreateToken](workflow/upstream_session_test.go) |
| `SessionTestSuite.TestCompletionFailed` | covered | Compiled session-from-closed/session-from-failed/session-activity-closed/session-completion-failure/session-timer cases assert fresh metadata, ordinary queue routing, closure after failure, and virtual time. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestUserTimerWithinSession` | covered | Compiled session-from-closed/session-from-failed/session-activity-closed/session-completion-failure/session-timer cases assert fresh metadata, ordinary queue routing, closure after failure, and virtual time. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) |
| `SessionTestSuite.TestActivityRetryWithinSession` | retained | Host SDK session-worker capacity and remote activity retry state machines remain unchanged. The bridge separately checks session queue routing. [TestSessionHostMetadata](temporalbridge/api_coverage_test.go) |

### internal/worker_version_sets_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_WorkerVersionSets_fromProtoResponse` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_VersioningIntentOld` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_VersioningIntent_EmptyTargetTQ` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/worker_versioning_rules_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_WorkerVersioningRules_fromProtoGetResponse` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_VersioningIntent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### internal/workflow_deadlock_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestDeadlockDetector` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDataConverterWithoutDeadlockDetection` | obsolete | `context_propagation`: The cooperative coroutine deadlock detector and its opt-out converter wrapper are absent from the native goroutine runtime. Disabling that removed detector has no native contract; native task budgets remain enforced. [TestResourcesCompiled](example/resources/check/main_test.go) |
| `TestDataConverterWithoutDeadlockDetectionContext` | obsolete | `context_propagation`: The removed cooperative detector wrapper has no native equivalent. Its independent WithContext/WithWorkflowContext masking assertions are covered by native context-aware helper and compiled tests; native task budgets remain enforced. [TestUpstreamNativeContextAwareDataConverter](workflow/context_converter_test.go) [TestUpstreamNativeContextPropagation](example/contextpropagation/check/main_test.go) |

### internal/workflow_integration_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWorkflowIntegrationHistoryMetadata` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) [TestPreviousRunSnapshotsPreservePresenceAndDecodeCodecs](temporalbridge/metadata_test.go) [TestLiveNamedRandomStreams](example/random/check/streams_test.go) |
| `TestWorkflowIntegrationSearchAttributeUpdates` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) [TestPreviousRunSnapshotsPreservePresenceAndDecodeCodecs](temporalbridge/metadata_test.go) |

### internal/workflow_random_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWorkflowRandomTestSuite` | suite | Suite entry point; individual suite test methods are inventoried separately. |
| `workflowRandomTestSuite.TestDeriveSeed` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestDeriveSeed](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestGetRandomStreamGolden` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestGetRandomStreamGolden](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestUint64Golden` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestUint64Golden](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestInterleavedReadUint64StableOrdering` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestInterleavedReadUint64StableOrdering](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestDeriveSeedSeparators` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestDeriveSeedSeparators](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestGetRandomStreamMemoizes` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestGetRandomStreamMemoizes](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestGetRandomStreamNamesAreIndependent` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestGetRandomStreamNamesAreIndependent](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestReseedRandomsInPlace` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestReseedRandomsInPlace](workflow/random_upstream_test.go) [TestCompiledNamedRandomStreams](example/random/check/streams_test.go) [TestLiveNamedRandomStreams](example/random/check/streams_test.go) |
| `workflowRandomTestSuite.TestChildContinueAsNewDrawsNewValues` | covered | Preserves upstream assertions using the native named source contract; seed and byte sequences match upstream exactly. [TestChildContinueAsNewDrawsNewValues](example/random/check/streams_upstream_test.go) [TestLiveNamedRandomStreams](example/random/check/streams_test.go) |

### internal/workflow_stream_reserved_name_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestIsWorkflowStreamReservedName` | covered | `workflow_streams`: Direct upstream port using the native stream package; workflow handlers execute in compiled isolates with context.Context and native channels. [TestIsWorkflowStreamReservedName](workflow/stream_namespace_test.go) |
| `TestIsWorkflowStreamReservedNameAllowsHandlers` | covered | `workflow_streams`: Direct upstream port using the native stream package; workflow handlers execute in compiled isolates with context.Context and native channels. [TestIsWorkflowStreamReservedNameAllowsHandlers](contrib/workflowstreams/native_test.go) |
| `TestIsWorkflowStreamReservedNameStillRejectsOtherNames` | covered | `workflow_streams`: Direct upstream port using the native stream package; workflow handlers execute in compiled isolates with context.Context and native channels. [TestIsWorkflowStreamReservedNameStillRejectsOtherNames](contrib/workflowstreams/native_test.go) |

### internal/workflow_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestGetChildWorkflowOptions` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamGetChildWorkflowOptions](workflow/upstream_options_test.go) |
| `TestGetActivityOptions` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamGetActivityOptions](workflow/upstream_options_test.go) |
| `TestGetLocalActivityOptions` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestUpstreamGetLocalActivityOptions](workflow/upstream_options_test.go) |
| `TestConvertRetryPolicy` | retained | Retry-policy protobuf conversion remains in the host SDK. The bridge option boundary is separately checked. [TestActivityOptionsReachSDKUnchanged](temporalbridge/activity_test.go) |
| `TestDeterministicKeys` | obsolete | Explicit key-sorting helper removed: native map range has compiler/runtime canonical ordering. [TestSavedHistoryChecksComputedTrace](example/determinism/check/main_test.go) |
| `TestDeterministicKeysFunc` | obsolete | Explicit key-sorting helper removed: native map range has compiler/runtime canonical ordering. [TestSavedHistoryChecksComputedTrace](example/determinism/check/main_test.go) |

### internal/workflow_testsuite_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestSetMemoOnStart` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `TestSetSearchAttributesOnStart` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `TestSetTypedSearchAttributesOnStart` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `TestNoExplicitRegistrationRequired` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestUnregisteredActivity` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestLocalActivityExecutionByActivityName` | covered | `test_environment`: Ports local activity execution and result decoding by registered function name. [TestLocalActivityExecutionByNameAndAlias](testsuite/upstream_test.go) |
| `TestLocalActivityExecutionByActivityNameAlias` | covered | `test_environment`: Ports local activity execution and result decoding by registered alias. [TestLocalActivityExecutionByNameAndAlias](testsuite/upstream_test.go) |
| `TestLocalActivityExecutionByActivityNameAliasMissingRegistration` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMissingLocalRegistrationResolvesFuture](temporalbridge/api_coverage_test.go) |
| `TestWorkflowIDInsideTestWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `TestWorkflowIDSignalWorkflowByID` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowIDUpdateWorkflowByID` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestChildWorkflowUpdate` | retained | `test_environment`: The unchanged host SDK test engine still owns child scheduling, IDs, retries and cancellation, together with this original test. The isolate facade supplies native definitions and child-start admission. Linked child and mock checks supplement the retained assertions without claiming a full native duplication of this source case. [TestCompiledChildWorkflows](example/child/check/main_test.go) [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) |
| `TestWorkflowUpdateOrder` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestWorkflowUpdateOrderWithOneArg` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestWorkflowUpdateOrderWithMultiArgs` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowUpdateIdGeneration` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowNotRegisteredRejected` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowUpdateOrderAcceptReject` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestWorkflowDuplicateIDDedup` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowDuplicateIDDedupInterweave` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowDuplicateIDDedupWithSleep` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowDuplicateIDDedupMore` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowDuplicateIDDedupDelayAndSleep` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowUpdateMissingCallbackFields` | retained | `test_environment`: The original harness assertions and their implementation remain in the delegated host SDK test engine. The isolate facade now reuses that engine with generated native entries. Linked compiled environment checks are supplemental boundary coverage, not an exhaustive native duplication of this source case. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestAllHandlersFinished` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestWorkflowAllHandlersFinished` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestWorkflowUpdateLogger` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledUpdatesAndReadOnlyValidators](example/update/check/main_test.go) |
| `TestWorkflowStartTimeInsideTestWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestMetadataAPIAndReplay](example/workflowmetadata/check/main_test.go) |
| `TestActivityAssertCalled` | covered | Real host environment activity mock assertions run against a compiled native workflow, including exact arguments and one call. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestActivityAssertNumberOfCalls` | covered | Real host environment activity mock assertions run against a compiled native workflow, including exact arguments and one call. [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestWorkflowMockingWithoutRegistration` | covered | `test_environment`: Ports function-valued workflow mocking without explicit registration and root execution by name. [TestWorkflowMockingWithoutRegistration](testsuite/upstream_test.go) |
| `TestActivityMockingByNameWithoutRegistrationFails` | retained | `test_environment`: The unchanged host SDK test engine still owns activity execution, retries, heartbeat/deadline handling and activity mocks/listeners, together with this original test. The isolate facade delegates those controls. Linked native harness checks supplement the retained assertions; they do not repeat every timeout/retry scenario. [TestNativeWorkflowMocksListenersAndAliases](testsuite/native_test.go) [TestUpstreamEnvironmentSDKPorts](example/upstream/check/main_test.go) |
| `TestMockCallWrapperInOrder` | covered | `test_environment`: Ports a local activity mock called before its prerequisite, including InOrder and the PanicError assertion. [TestMockCallWrapperOrderFailures](testsuite/upstream_test.go) |
| `TestMockCallWrapperNotBefore` | covered | `test_environment`: Ports a local activity mock called before its prerequisite, including NotBefore and the PanicError assertion. [TestMockCallWrapperOrderFailures](testsuite/upstream_test.go) |
| `TestCustomFailureConverter` | covered | `custom_failure_converter`: The compiled native port reconstructs the custom error type and asserts that its NonRetryable flag limits activity execution to one attempt. Additional checks cover workflow/local/child/update failures, read-only validators/previous-run queries, precise details, per-context data converters, factory validation and compressed saved-history replay. [TestUpstreamCustomFailureConverter](example/failureconversion/check/main_test.go) [TestCustomFailureHistoryReplay](example/failureconversion/check/main_test.go) [TestCustomFailureRoundTripAndCancellation](internal/failurecodec/failurecodec_test.go) |
| `TestDynamicWorkflows` | covered | `dynamic_handlers`: Native dynamic workflow dispatch and host dynamic activities are exercised through the exported test environment. Additional coverage verifies dynamic children, named precedence, arbitrary query/update names, converter/interceptor ownership, read-only validators, replacement, precise results, live cache eviction and recorded history replay. [TestDynamicWorkflows](example/dynamic/check/native_test.go) [TestNativeDynamicQueryAndUpdateHandlers](example/dynamic/check/native_test.go) [TestDynamicHistoryReplay](example/dynamic/check/main_test.go) |
| `TestRequestCancelExternalWorkflowInSelector` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |
| `TestExecuteActivitiesInWorkflow` | covered | Source behavior exercised through native SDK APIs or the copied-byte host bridge. [TestCompiledAPICoverageAndReplay](example/apicoverage/check/main_test.go) [TestUpstreamNativeSDKPorts](example/upstream/check/main_test.go) |

### internalbindings/internalbindings_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNewNexusClient_ExposesEndpointAndService` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewExecuteNexusOperationParams_RoundtripsAllFields` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewExecuteNexusOperationParams_AcceptsNilHeader` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### log/slog_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestTemporalSlogAdapter` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### log/with_logger_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWithLogger` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestWithLoggerSkip` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### mocks/mock_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `Test_MockClient` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_MockResetWorkflowExecution` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `Test_MockScheduleClient` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### temporal/error_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestIsWorkflowExecutionAlreadyStartedError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewPayloadValidationError` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### temporalnexus/link_converter_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestConvertLinkWorkflowEventToNexusLink` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestConvertNexusLinkToLinkWorkflowEvent` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestConvertLinkNexusOperationToNexusLink` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestConvertNexusLinkToLinkNexusOperation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### temporalnexus/operation_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNewWorkflowRunOperationWithOptions` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### temporalnexus/temporal_operation_go1.27_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNexusClientStartWorkflowSmoke` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusClientStartUpdateWorkflowSmoke` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusClientStartActivitySmoke` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusClientStartWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusClientStartUpdateWorkflow` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNexusClientStartActivity` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### temporalnexus/temporal_operation_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNewSyncResult` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewAsyncResult` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestToHandlerResultSync` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestToHandlerResultAsync` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestToHandlerResultBothSet` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestToHandlerResultNeitherSet` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewTemporalOperationValidation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMustNewTemporalOperationPanics` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestMustNewTemporalOperationValid` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestLoadTokenType` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestNewTemporalOperationDefaultsCancelActivityExecution` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDoubleStartGuard` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStartUpdateWorkflowGuards` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### temporalnexus/token_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestEncodeDecodeWorkflowRunOperationToken` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncodeWorkflowRunOperationTokenDoesNotIncludeVersion` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncodeDecodeUpdateWorkflowOperationToken` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncodeDecodeActivityExecutionOperationToken` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncodeActivityExecutionOperationTokenDoesNotIncludeVersion` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestEncodeActivityExecutionOperationTokenOmitsRunIDWhenEmpty` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecodeActivityExecutionOperationTokenErrors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestDecodeWorkflowRunOperationTokenErrors` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### testsuite/devserver_internal_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestWaitServerReady_respectsTimeout` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestRetryFor_respectsCancellation` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### testsuite/devserver_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestStartDevServer_Defaults` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStartDevServer_SpecificVersion` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStartDevServer_CustomNamespace` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStartDevServer_FrontendHostPort` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
| `TestStartDevServer_SearchAttributes` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |

### worker/worker_test.go

| Source test | Disposition | Coverage / blocker / reason |
| --- | --- | --- |
| `TestNew_NegativeMaxConcurrentWorkflowTaskExternalStorageVisits` | retained | The host SDK still owns this implementation and its unit tests; worker.Worker/Options alias and embed upstream types, and temporalbridge delegates through WorkflowEnvironment. No duplicate native implementation to port. |
