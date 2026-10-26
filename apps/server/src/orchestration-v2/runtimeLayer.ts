import * as GuestShareService from "../guest/GuestShareService.ts";
import * as UsageLimitRecoveryWorker from "./UsageLimitRecoveryWorker.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Layer from "effect/Layer";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as CheckoutAdmission from "../workspace/CheckoutAdmission.ts";
import * as CheckoutAdmissionLive from "../workspace/CheckoutAdmissionLive.ts";
import * as CheckoutIdentity from "../workspace/CheckoutIdentity.ts";
import * as CheckoutOwnershipStore from "../workspace/CheckoutOwnershipStore.ts";
import * as CheckoutOwnershipService from "../workspace/CheckoutOwnershipService.ts";
import * as CheckoutOwnershipRecovery from "../workspace/CheckoutOwnershipRecovery.ts";
import * as CheckoutProcessAccounting from "../workspace/CheckoutProcessAccounting.ts";
import * as CheckoutWaitService from "../workspace/CheckoutWaitService.ts";
import * as WorkspaceExecutionBindings from "../workspace/WorkspaceExecutionBindings.ts";
import * as VerificationFingerprint from "../verification/VerificationFingerprint.ts";
import * as VerificationSourceObserver from "../verification/VerificationSourceObserver.ts";
import * as VerificationRunner from "../verification/VerificationRunner.ts";
import * as VerificationStore from "../verification/VerificationStore.ts";
import * as VerificationInvalidationService from "../verification/VerificationInvalidationService.ts";
import * as VerificationService from "../verification/VerificationService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as OrchestrationCommandReceipts from "../persistence/OrchestrationCommandReceipts.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as McpAppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as McpAppRequests from "../mcpApps/McpAppRequests.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as ProviderAuthService from "../provider/ProviderAuthService.ts";
import * as AgentSessionImporter from "../project/AgentSessionImporter.ts";
import * as AgentSessionScanner from "../project/AgentSessionScanner.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as CheckpointCaptureService from "./CheckpointCaptureService.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as CommandPolicy from "./CommandPolicy.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ContextHandoffService from "./ContextHandoffService.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as CommandOutputHub from "./CommandOutputHub.ts";
import * as ProviderRuntimeRecoveryService from "./ProviderRuntimeRecoveryService.ts";
import * as ProviderSwitchService from "./ProviderSwitchService.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadLifecycleService from "./ThreadLifecycleService.ts";
import * as ThreadForkService from "./ThreadForkService.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";

export const layerEventInfrastructure = Layer.mergeAll(
  OrchestrationEventStore.layer,
  OrchestrationCommandReceipts.layer,
);

const layerRuntimePolicyProvided = RuntimePolicy.layerFromProjectStore.pipe(
  Layer.provide(ProjectStore.layer),
);

const layerEventStoreProvided = EventStore.layerFromOrchestrationEventStore.pipe(
  Layer.provide(layerEventInfrastructure),
);
const layerCommandReceiptStoreProvided = CommandReceiptStore.layerFromApplicationReceipts.pipe(
  Layer.provide(layerEventInfrastructure),
);

const layerStores = Layer.mergeAll(
  layerEventInfrastructure,
  layerEventStoreProvided,
  ProjectionStore.layer,
  ProjectStore.layer,
  layerCommandReceiptStoreProvided,
  EffectOutbox.layer,
  TurnItemPositionStore.layer,
);

export const layerEventSink = EventSink.layerFromStores.pipe(Layer.provide(layerStores));
const layerEventSinkProvided = layerEventSink;
const layerProjectionMaintenanceProvided = ProjectionMaintenance.layer.pipe(
  Layer.provide(layerStores),
);
const layerLegacyV1ThreadImporterProvided = LegacyV1ThreadImporter.layer.pipe(
  Layer.provide(layerEventSinkProvided),
);

export const layerProjectService = ProjectService.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      ProjectStore.layer,
      ProjectionStore.layer,
      layerEventSinkProvided,
      IdAllocator.layer,
      layerLegacyV1ThreadImporterProvided,
    ),
  ),
);

const layerProviderEventIngestorProvided = ProviderEventIngestor.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      layerEventSinkProvided,
      IdAllocator.layer,
      ProjectionStore.layer,
      ThreadCommandExecutor.layer,
      GitVcsDriver.layer,
    ),
  ),
);

const layerCheckpointServiceProvided = CheckpointService.layer.pipe(
  Layer.provide(IdAllocator.layer),
);
const layerContextHandoffServiceProvided = ContextHandoffService.layer.pipe(
  Layer.provide(IdAllocator.layer),
);

const layerProviderAdapterRegistryProvided =
  ProviderAdapterRegistry.layerFromProviderInstanceRegistry;
const layerProviderSwitchServiceProvided = ProviderSwitchService.layer.pipe(
  Layer.provide(layerProviderAdapterRegistryProvided),
);

const layerCommandOutputHubProvided = CommandOutputHub.layer.pipe(
  Layer.provide(Layer.merge(layerEventSinkProvided, ProjectionStore.layer)),
);

const layerProviderSessionManagerProvided = ProviderSessionManager.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      layerCommandOutputHubProvided,
      layerProviderAdapterRegistryProvided,
      layerEventSinkProvided,
      IdAllocator.layer,
      layerProviderEventIngestorProvided,
      ProjectionStore.layer,
    ),
  ),
);

const layerProviderAuthServiceProvided = ProviderAuthService.layer.pipe(
  Layer.provide(Layer.merge(ProjectionStore.layer, layerProviderSessionManagerProvided)),
);

// Core test harnesses retain default reference behavior. Production supplies the same live
// admission layer to each constructor, before any late verification or wait service exists.
function makeRuntimeLayers<AdmissionServices, AdmissionError, AdmissionRequirements>(
  admissionLayer: Layer.Layer<AdmissionServices, AdmissionError, AdmissionRequirements>,
) {
  const layerRunExecutionServiceProvided = RunExecutionService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        McpAppModelContext.layer,
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        layerProviderEventIngestorProvided,
        TurnItemPositionStore.layer,
      ),
    ),
    Layer.provide(admissionLayer),
  );

  const layerProviderTurnStartServiceProvided = ProviderTurnStartService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerContextHandoffServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        ProjectionStore.layer,
        layerProviderSessionManagerProvided,
        layerProviderAuthServiceProvided,
        layerRunExecutionServiceProvided,
        layerRuntimePolicyProvided,
      ),
    ),
    Layer.provide(admissionLayer),
  );

  const layerProviderTurnControlServiceProvided = ProviderTurnControlService.layer.pipe(
    Layer.provide(Layer.merge(ProjectionStore.layer, layerProviderSessionManagerProvided)),
  );
  const layerRuntimeRequestServiceProvided = RuntimeRequestService.layer.pipe(
    Layer.provide(Layer.merge(ProjectionStore.layer, layerProviderSessionManagerProvided)),
  );
  const layerCheckpointRollbackServiceProvided = CheckpointRollbackService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ProjectStore.layer,
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        ProjectionStore.layer,
        layerProviderSessionManagerProvided,
        ThreadCommandExecutor.layer,
        layerRuntimePolicyProvided,
      ),
    ),
  );
  const layerCheckpointCaptureServiceProvided = CheckpointCaptureService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        ProjectionStore.layer,
      ),
    ),
  );
  const layerRunFinalizationServiceProvided = RunFinalizationService.layer
    .pipe(Layer.provide(Layer.merge(layerCheckpointCaptureServiceProvided, ProjectionStore.layer)))
    .pipe(Layer.provide(admissionLayer));

  const layerOrchestratorProvided = Orchestrator.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerCheckpointServiceProvided,
        CommandPolicy.layer,
        layerStores,
        layerEventSinkProvided,
        layerCommandReceiptStoreProvided,
        layerContextHandoffServiceProvided,
        IdAllocator.layer,
        ProjectStore.layer,
        layerProviderAdapterRegistryProvided,
        // Reuse the continuation worker's layer reference so memoization shares one request queue.
        ProviderContinuationRequests.layer,
        layerProviderEventIngestorProvided,
        layerRuntimePolicyProvided,
        layerProviderSessionManagerProvided,
        layerProviderSwitchServiceProvided,
        layerRunExecutionServiceProvided,
        ThreadForkService.layer,
      ),
    ),
  );

  const layerAgentSessionImporterProvided = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        AgentSessionScanner.layer,
        layerProjectService,
        layerOrchestratorProvided,
        layerEventSinkProvided,
        IdAllocator.layer,
        ProviderSessionRuntime.layer,
      ),
    ),
  );

  const layerThreadManagementProvided = ThreadManagementService.layerWithLegacyImporter.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerOrchestratorProvided,
        layerLegacyV1ThreadImporterProvided,
        layerCommandReceiptStoreProvided,
        layerProviderSessionManagerProvided,
      ),
    ),
  );
  const layerProjectSetupScriptRunner = ProjectSetupScriptRunner.layer.pipe(
    Layer.provide(layerProjectService),
  );
  const layerManagedProjectFoldersProvided = ManagedProjectFolders.layer.pipe(
    Layer.provide(layerProjectService),
  );
  const layerThreadLaunchProvided = ThreadLaunchService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerProjectService,
        layerProjectSetupScriptRunner,
        layerManagedProjectFoldersProvided,
        layerThreadManagementProvided,
        layerCommandReceiptStoreProvided,
        IdAllocator.layer,
      ),
    ),
    Layer.provide(admissionLayer),
  );
  const layerThreadLifecycleProvided = ThreadLifecycleService.layer.pipe(
    Layer.provide(layerThreadManagementProvided),
  );
  const layerSecretRequestsProvided = SecretRequests.layer.pipe(
    Layer.provide(layerThreadManagementProvided),
  );
  const layerScheduledTaskProvided = ScheduledTaskService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerThreadLaunchProvided,
        layerThreadManagementProvided,
        layerManagedProjectFoldersProvided,
        layerSecretRequestsProvided,
      ),
    ),
  );
  const layerProviderContinuationWorkerProvided = ProviderContinuationService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ProviderContinuationRequests.layer,
        layerThreadManagementProvided,
        IdAllocator.layer,
      ),
    ),
  );
  const layerThreadTitleRegenerationProvided = ThreadTitleRegenerationService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(layerThreadManagementProvided, ProjectStore.layer, TextGeneration.layer),
    ),
  );
  const complete = <Bindings, BindingsError, BindingsRequirements>(
    bindings: Layer.Layer<Bindings, BindingsError, BindingsRequirements>,
  ) => {
    const layerEffectExecutorProvided = EffectWorker.layerExecutor.pipe(
      Layer.provide(
        Layer.mergeAll(
          layerRunFinalizationServiceProvided,
          layerCheckpointRollbackServiceProvided,
          layerProviderSessionManagerProvided,
          layerProviderTurnControlServiceProvided,
          layerProviderTurnStartServiceProvided,
          layerRuntimeRequestServiceProvided,
          layerThreadTitleRegenerationProvided,
          layerThreadManagementProvided,
        ),
      ),
      Layer.provide(bindings),
    );
    const layerEffectWorkerProvided = EffectWorker.layer.pipe(
      Layer.provide(Layer.merge(layerStores, layerEffectExecutorProvided)),
      Layer.provide(bindings),
    );
    const layerProviderRuntimeRecoveryProvided = ProviderRuntimeRecoveryService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          layerEffectWorkerProvided,
          layerStores,
          layerEventSinkProvided,
          IdAllocator.layer,
          ProjectionStore.layer,
        ),
      ),
    );

    const layerMcpAppRequestsProvided = McpAppRequests.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layer,
          layerOrchestratorProvided,
          layerThreadManagementProvided,
          layerProviderSessionManagerProvided,
        ),
      ),
    );

    const layer = Layer.mergeAll(
      layerCommandOutputHubProvided,
      layerOrchestratorProvided,
      layerMcpAppRequestsProvided,
      layerThreadManagementProvided,
      layerEffectWorkerProvided,
      layerProviderSessionManagerProvided,
      layerProviderAuthServiceProvided,
      layerProviderRuntimeRecoveryProvided,
      layerProjectionMaintenanceProvided,
      layerLegacyV1ThreadImporterProvided,
    );

    const layerProduction = Layer.mergeAll(
      layer.pipe(Layer.provide(layerProjectService)),
      layerProjectService,
      layerManagedProjectFoldersProvided,
      layerThreadLaunchProvided,
      layerThreadLifecycleProvided,
      layerScheduledTaskProvided,
      layerSecretRequestsProvided,
      GuestShareService.layer.pipe(
        Layer.provide(Layer.merge(layerOrchestratorProvided, layerCommandOutputHubProvided)),
      ),
      UsageLimitRecoveryWorker.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            ProjectionStore.layer,
            layerThreadManagementProvided,
            layerOrchestratorProvided,
          ),
        ),
      ),
      layerProviderContinuationWorkerProvided,
      layerAgentSessionImporterProvided,
      EffectOutbox.layerPruneWorker.pipe(Layer.provide(EffectOutbox.layer)),
    ).pipe(
      Layer.provide(ResourceCleanupService.layer.pipe(Layer.provide(layerScheduledTaskProvided))),
      Layer.provide(Scheduler.layer),
      Layer.provideMerge(layerEventInfrastructure),
    );
    return { layer, layerProduction: layerProduction.pipe(Layer.provideMerge(bindings)) };
  };
  return {
    complete,
    threadManagement: layerThreadManagementProvided,
    threadLaunch: layerThreadLaunchProvided,
    setup: layerProjectSetupScriptRunner,
  };
}

const legacy = makeRuntimeLayers(
  Layer.effect(CheckoutAdmission.CheckoutAdmission, CheckoutAdmission.CheckoutAdmission),
);
export const layer = legacy.complete(Layer.empty).layer;
export const layerProjectSetupScriptRunner = legacy.setup;

// Every consumer reuses these exact layer objects, sharing ownership locks and store notifications.
const layerOwnership = CheckoutOwnershipService.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      CheckoutOwnershipStore.layer,
      CheckoutProcessAccounting.layer,
      CheckoutIdentity.layer.pipe(Layer.provide(ProcessRunner.layer)),
      ServerEnvironment.layerIdentity,
    ),
  ),
);
const layerRecovery = CheckoutOwnershipRecovery.layer.pipe(Layer.provideMerge(layerOwnership));
const layerAdmission = CheckoutAdmissionLive.layer.pipe(
  Layer.provideMerge(layerRecovery),
  Layer.provide(
    Layer.mergeAll(
      ProjectStore.layer,
      ProjectionStore.layer,
      layerEventSinkProvided,
      IdAllocator.layer,
    ),
  ),
);
const production = makeRuntimeLayers(layerAdmission);
const layerEvidenceStore = VerificationStore.layer.pipe(Layer.provide(EffectOutbox.layer));
const layerInvalidation = VerificationInvalidationService.layer.pipe(
  Layer.provideMerge(layerEvidenceStore),
);
const layerVerificationRunner = VerificationRunner.layer.pipe(
  Layer.provideMerge(Layer.merge(VerificationFingerprint.layer, VerificationSourceObserver.layer)),
  Layer.provideMerge(layerOwnership),
  Layer.provide(ProcessRunner.layer),
);
const layerVerification = VerificationService.layer.pipe(
  Layer.provideMerge(layerInvalidation),
  Layer.provideMerge(layerVerificationRunner),
  Layer.provideMerge(layerAdmission),
  Layer.provide(
    Layer.mergeAll(
      layerRuntimePolicyProvided,
      ProjectStore.layer,
      ProjectionStore.layer,
      EffectOutbox.layer,
    ),
  ),
);
const layerWaits = CheckoutWaitService.layer.pipe(
  Layer.provideMerge(layerAdmission),
  Layer.provide(production.threadManagement),
  Layer.provide(
    Layer.mergeAll(
      layerEventSinkProvided,
      ProjectionStore.layer,
      EffectOutbox.layer,
      IdAllocator.layer,
    ),
  ),
);
const layerBindings = WorkspaceExecutionBindings.layer.pipe(
  Layer.provideMerge(Layer.merge(layerVerification, layerWaits)),
  Layer.provide(Layer.mergeAll(production.threadLaunch, layerEventSinkProvided, IdAllocator.layer)),
);
export const layerProduction = production.complete(layerBindings).layerProduction;
