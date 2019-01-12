using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.CodeGeneration;
using Orleans.Configuration;
using Orleans.Runtime.GrainDirectory;
using Orleans.Runtime.Messaging;
using Orleans.Runtime.Placement;
using Orleans.Runtime.Scheduler;
using Orleans.Runtime.Versions.Compatibility;
using Orleans.Serialization;

namespace Orleans.Runtime
{
    internal class Dispatcher
    {
        internal ISiloMessageCenter Transport { get; }

        private readonly OrleansTaskScheduler scheduler;
        private readonly Catalog catalog;
        private readonly ILogger logger;
        private readonly SiloMessagingOptions messagingOptions;
        private readonly PlacementDirectorsManager placementDirectorsManager;
        private readonly ILocalGrainDirectory localGrainDirectory;
        private readonly ActivationCollector activationCollector;
        private readonly MessageFactory messageFactory;
        private readonly SerializationManager serializationManager;
        private readonly CompatibilityDirectorManager compatibilityDirectorManager;
        private readonly SchedulingOptions schedulingOptions;
        internal Dispatcher(
            OrleansTaskScheduler scheduler, 
            ISiloMessageCenter transport, 
            Catalog catalog,
            IOptions<SiloMessagingOptions> messagingOptions,
            PlacementDirectorsManager placementDirectorsManager,
            ILocalGrainDirectory localGrainDirectory,
            ActivationCollector activationCollector,
            MessageFactory messageFactory,
            SerializationManager serializationManager,
            CompatibilityDirectorManager compatibilityDirectorManager,
            ILoggerFactory loggerFactory,
            IOptions<SchedulingOptions> schedulerOptions)
        {
            this.scheduler = scheduler;
            this.catalog = catalog;
            Transport = transport;
            this.messagingOptions = messagingOptions.Value;
            this.placementDirectorsManager = placementDirectorsManager;
            this.localGrainDirectory = localGrainDirectory;
            this.activationCollector = activationCollector;
            this.messageFactory = messageFactory;
            this.serializationManager = serializationManager;
            this.compatibilityDirectorManager = compatibilityDirectorManager;
            this.schedulingOptions = schedulerOptions.Value;
            logger = loggerFactory.CreateLogger<Dispatcher>();
        }

        public ISiloRuntimeClient RuntimeClient => this.catalog.RuntimeClient;

        /// <summary>
        /// Receive a new message:
        /// - validate order constraints, queue (or possibly redirect) if out of order
        /// - validate transactions constraints
        /// - invoke handler if ready, otherwise enqueue for later invocation
        /// </summary>
        /// <param name="message"></param>
        public void ReceiveMessage(Message message)
        {
            MessagingProcessingStatisticsGroup.OnDispatcherMessageReceive(message);
            // Don't process messages that have already timed out
            if (message.IsExpired)
            {
                logger.Warn(ErrorCode.Dispatcher_DroppingExpiredMessage, "Dropping an expired message: {0}", message);
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Expired");
                message.DropExpiredMessage(MessagingStatisticsGroup.Phase.Dispatch);
                return;
            }

            // check if its targeted at a new activation
            if (message.TargetGrain.IsSystemTarget)
            {
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "ReceiveMessage on system target.");
                throw new InvalidOperationException("Dispatcher was called ReceiveMessage on system target for " + message);
            }

            try
            {
                Task ignore;
                ActivationData target = catalog.GetOrCreateActivation(
                    message.TargetAddress, 
                    message.IsNewPlacement,
                    message.NewGrainType,
                    String.IsNullOrEmpty(message.GenericGrainType) ? null : message.GenericGrainType, 
                    message.RequestContextData,
                    out ignore);

                if (ignore != null)
                {
                    ignore.Ignore();
                }

                if (message.Direction == Message.Directions.Response)
                {
                    ReceiveResponse(message, target);
                }
                else // Request or OneWay
                {
                    if (target.State == ActivationState.Valid)
                    {
                        this.activationCollector.TryRescheduleCollection(target);
                    }
                    // Silo is always capable to accept a new request. It's up to the activation to handle its internal state.
                    // If activation is shutting down, it will queue and later forward this request.
                    ReceiveRequest(message, target);
                }
            }
            catch (Exception ex)
            {
                try
                {
                    MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Non-existent activation");
              
                    var nea = ex as Catalog.NonExistentActivationException;
                    if (nea == null)
                    {
                        var str = $"Error creating activation for {message.NewGrainType}. Message {message}";
                        logger.Error(ErrorCode.Dispatcher_ErrorCreatingActivation, str, ex);
                        throw new OrleansException(str, ex);
                    }

                    if (nea.IsStatelessWorker)
                    {
                        if (logger.IsEnabled(LogLevel.Debug)) logger.Debug(ErrorCode.Dispatcher_Intermediate_GetOrCreateActivation,
                           $"Intermediate StatelessWorker NonExistentActivation for message {message}, Exception {ex}");
                    }
                    else
                    {
                        logger.Info(ErrorCode.Dispatcher_Intermediate_GetOrCreateActivation,
                            $"Intermediate NonExistentActivation for message {message}, with Exception {ex}");
                    }

                    ActivationAddress nonExistentActivation = nea.NonExistentActivation;

                    if (message.Direction != Message.Directions.Response)
                    {
                        // Un-register the target activation so we don't keep getting spurious messages.
                        // The time delay (one minute, as of this writing) is to handle the unlikely but possible race where
                        // this request snuck ahead of another request, with new placement requested, for the same activation.
                        // If the activation registration request from the new placement somehow sneaks ahead of this un-registration,
                        // we want to make sure that we don't un-register the activation we just created.
                        // We would add a counter here, except that there's already a counter for this in the Catalog.
                        // Note that this has to run in a non-null scheduler context, so we always queue it to the catalog's context
                        var origin = message.SendingSilo;
                        scheduler.QueueWorkItem(new ClosureWorkItem(
                            // don't use message.TargetAddress, cause it may have been removed from the headers by this time!
                            async () =>
                            {
                                try
                                {
                                    await this.localGrainDirectory.UnregisterAfterNonexistingActivation(
                                        nonExistentActivation, origin);
                                }
                                catch (Exception exc)
                                {
                                    logger.Warn(ErrorCode.Dispatcher_FailedToUnregisterNonExistingAct,
                                        $"Failed to un-register NonExistentActivation {nonExistentActivation}", exc);
                                }
                            },
                            "LocalGrainDirectory.UnregisterAfterNonexistingActivation"),
                            catalog.SchedulingContext);

                        ProcessRequestToInvalidActivation(message, nonExistentActivation, null, "Non-existent activation");
                    }
                    else
                    {
                        logger.Warn(
                            ErrorCode.Dispatcher_NoTargetActivation,
                            nonExistentActivation.Silo.IsClient
                                ? "No target client {0} for response message: {1}. It's likely that the client recently disconnected."
                                : "No target activation {0} for response message: {1}",
                            nonExistentActivation,
                            message);

                        this.localGrainDirectory.InvalidateCacheEntry(nonExistentActivation);
                    }
                }
                catch (Exception exc)
                {
                    // Unable to create activation for this request - reject message
                    RejectMessage(message, Message.RejectionTypes.Transient, exc);
                }
            }
        }

        public void RejectMessage(
            Message message, 
            Message.RejectionTypes rejectType, 
            Exception exc, 
            string rejectInfo = null)
        {
            if (message.Direction == Message.Directions.Request)
            {
                var str = String.Format("{0} {1}", rejectInfo ?? "", exc == null ? "" : exc.ToString());
                MessagingStatisticsGroup.OnRejectedMessage(message);
                Message rejection = this.messageFactory.CreateRejectionResponse(message, rejectType, str, exc);
                SendRejectionMessage(rejection);
            }
            else
            {
                logger.Warn(ErrorCode.Messaging_Dispatcher_DiscardRejection,
                    "Discarding {0} rejection for message {1}. Exc = {2}",
                    Enum.GetName(typeof(Message.Directions), message.Direction), message, exc == null ? "" : exc.Message);
            }
        }

        internal void SendRejectionMessage(Message rejection)
        {
            if (rejection.Result == Message.ResponseTypes.Rejection)
            {
                Transport.SendMessage(rejection);
                rejection.ReleaseBodyAndHeaderBuffers();
            }
            else
            {
                throw new InvalidOperationException(
                    "Attempt to invoke Dispatcher.SendRejectionMessage() for a message that isn't a rejection.");
            }
        }

        private void ReceiveResponse(Message message, ActivationData targetActivation)
        {
            lock (targetActivation)
            {
                if (targetActivation.State == ActivationState.Invalid)
                {
                    logger.Warn(ErrorCode.Dispatcher_Receive_InvalidActivation,
                        "Response received for invalid activation {0}", message);
                    MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Invalid");
                    return;
                }
                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedOk(message);
                if (Transport.TryDeliverToProxy(message)) return;

               this.catalog.RuntimeClient.ReceiveResponse(message);
            }
        }

        /// <summary>
        /// Check if we can locally accept this message.
        /// Redirects if it can't be accepted.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="targetActivation"></param>
        private void ReceiveRequest(Message message, ActivationData targetActivation)
        {
            lock (targetActivation)
            {
                if (targetActivation.State == ActivationState.Invalid)
                {
                    this.ProcessRequestToInvalidActivation(
                        message,
                        targetActivation.Address,
                        targetActivation.ForwardingAddress,
                        "ReceiveRequest");
                    return;
                }

                if (targetActivation.Grain.IsGrain && message.IsUsingInterfaceVersions)
                {
                    var request = ((InvokeMethodRequest)message.GetDeserializedBody(this.serializationManager));
                    var compatibilityDirector = compatibilityDirectorManager.GetDirector(request.InterfaceId);
                    var currentVersion = catalog.GrainTypeManager.GetLocalSupportedVersion(request.InterfaceId);
                    if (!compatibilityDirector.IsCompatible(request.InterfaceVersion, currentVersion))
                        catalog.DeactivateActivationOnIdle(targetActivation);
                }

                if (targetActivation.State == ActivationState.Invalid || targetActivation.State == ActivationState.Deactivating)
                {
                    ProcessRequestToInvalidActivation(message, targetActivation.Address, targetActivation.ForwardingAddress, "HandleIncomingRequest");
                    return;
                }

                try
                {
                    switch (targetActivation.ScheduleRequest(message))
                    {
                        case ActivationRequestSchedulerResult.Success:
                            // Great, nothing to do
                            break;
                        case ActivationRequestSchedulerResult.ErrorInvalidActivation:
                            ProcessRequestToInvalidActivation(message, targetActivation.Address, targetActivation.ForwardingAddress, "EnqueueRequest");
                            break;
                        case ActivationRequestSchedulerResult.ErrorStuckActivation:
                            // Avoid any new call to this activation
                            catalog.DeactivateStuckActivation(targetActivation);
                            ProcessRequestToInvalidActivation(message, targetActivation.Address, targetActivation.ForwardingAddress, "EnqueueRequest - blocked grain");
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                catch (DeadlockException deadlockException)
                {
                    // Record that this message is no longer flowing through the system
                    MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Deadlock");
                    this.logger.Warn(ErrorCode.Dispatcher_DetectedDeadlock,
                        "Detected Application Deadlock: {0}", deadlockException.Message);
                    // We want to send DeadlockException back as an application exception, rather than as a system rejection.
                    this.SendResponse(message, Response.ExceptionResponse(deadlockException));
                }
                catch(LimitExceededException overloadException)
                {
                    MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "Overload2");
                    RejectMessage(message, Message.RejectionTypes.Overloaded, overloadException, "Target activation is overloaded " + targetActivation);
                }
            }
        }

        /// <summary>
        /// https://github.com/dotnet/orleans/issues/3184
        /// Checks whether reentrancy is allowed for calls to grains that are already part of the call chain.
        /// Covers following case: grain A calls grain B, and while executing the invoked method B calls back to A. 
        /// Design: Senders collection `RunningRequestsSenders` contains sending grains references
        /// during duration of request processing. If target of outgoing request is found in that collection - 
        /// such request will be marked as interleaving in order to prevent deadlocks.
        /// </summary>
        private void MarkSameCallChainMessageAsInterleaving(ActivationData sendingActivation, Message outgoing)
        {
            if (!schedulingOptions.AllowCallChainReentrancy)
            {
                return;
            }

            if (sendingActivation?.RunningRequestsSenders.Contains(outgoing.TargetActivation) == true)
            {
                outgoing.IsAlwaysInterleave = true;
            }
        }

        internal void ProcessRequestToInvalidActivation(
            Message message, 
            ActivationAddress oldAddress, 
            ActivationAddress forwardingAddress, 
            string failedOperation, 
            Exception exc = null)
        {
            // Just use this opportunity to invalidate local Cache Entry as well. 
            if (oldAddress != null)
            {
                this.localGrainDirectory.InvalidateCacheEntry(oldAddress);
            }
            // IMPORTANT: do not do anything on activation context anymore, since this activation is invalid already.
            this.scheduler.QueueWorkItem(new ClosureWorkItem(
                () => this.TryForwardRequest(message, oldAddress, forwardingAddress, failedOperation, exc)),
                this.catalog.SchedulingContext);
        }

        internal void ProcessRequestsToInvalidActivation(
            List<Message> messages,
            ActivationAddress oldAddress,
            ActivationAddress forwardingAddress, 
            string failedOperation,
            Exception exc = null,
            bool rejectMessages = false)
        {
            // Just use this opportunity to invalidate local Cache Entry as well. 
            if (oldAddress != null)
            {
                this.localGrainDirectory.InvalidateCacheEntry(oldAddress);
            }

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.Info(ErrorCode.Messaging_Dispatcher_ForwardingRequests,
                    $"Forwarding {messages.Count} requests destined for address {oldAddress} to address {forwardingAddress} after {failedOperation}.");
            }

            // IMPORTANT: do not do anything on activation context anymore, since this activation is invalid already.
            scheduler.QueueWorkItem(new ClosureWorkItem(
                () =>
                {
                    foreach (var message in messages)
                    {
                        if (rejectMessages)
                        {
                            RejectMessage(message, Message.RejectionTypes.Transient, exc, failedOperation);
                        }
                        else
                        {
                            TryForwardRequest(message, oldAddress, forwardingAddress, failedOperation, exc);
                        }
                        
                    }
                }
                ), catalog.SchedulingContext);
        }

        internal void TryForwardRequest(Message message, ActivationAddress oldAddress, ActivationAddress forwardingAddress, string failedOperation, Exception exc = null)
        {
            bool forwardingSucceded = true;
            try
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation(
                        (int)ErrorCode.Messaging_Dispatcher_TryForward,
                        "Trying to forward after {FailedOperation}, ForwardCount = {ForwardCount}. OldAddress = {OldAddress}, ForwardingAddress = {ForwardingAddress}, Message {Message}, Exception: {Exception}.",
                        failedOperation,
                        message.ForwardCount,
                        oldAddress,
                        forwardingAddress,
                        message,
                        exc);
                }

                // if this message is from a different cluster and hit a non-existing activation
                // in this cluster (which can happen due to stale cache or directory states)
                // we forward it back to the original silo it came from in the original cluster,
                // and target it to a fictional activation that is guaranteed to not exist.
                // This ensures that the GSI protocol creates a new instance there instead of here.
                if (forwardingAddress == null
                    && message.TargetSilo != message.SendingSilo
                    && !this.localGrainDirectory.IsSiloInCluster(message.SendingSilo))
                {
                    message.IsReturnedFromRemoteCluster = true; // marks message to force invalidation of stale directory entry
                    forwardingAddress = ActivationAddress.NewActivationAddress(message.SendingSilo, message.TargetGrain);
                    logger.Info(ErrorCode.Messaging_Dispatcher_ReturnToOriginCluster, $"Forwarding back to origin cluster, to fictional activation {message}");
                }

                MessagingProcessingStatisticsGroup.OnDispatcherMessageReRouted(message);
                if (oldAddress != null)
                {
                    message.AddToCacheInvalidationHeader(oldAddress);
                }

                forwardingSucceded = this.TryForwardMessage(message, forwardingAddress);
            }
            catch (Exception exc2)
            {
                forwardingSucceded = false;
                exc = exc2;
            }
            finally
            {
                if (!forwardingSucceded)
                {
                    var str = $"Forwarding failed: tried to forward message {message} for {message.ForwardCount} times after {failedOperation} to invalid activation. Rejecting now.";
                    logger.Warn(ErrorCode.Messaging_Dispatcher_TryForwardFailed, str, exc);
                    RejectMessage(message, Message.RejectionTypes.Transient, exc, str);
                }
            }
        }

        /// <summary>
        /// Reroute a message coming in through a gateway
        /// </summary>
        /// <param name="message"></param>
        internal void RerouteMessage(Message message)
        {
            ResendMessageImpl(message);
        }

        internal bool TryResendMessage(Message message)
        {
            if (!message.MayResend(this.messagingOptions.MaxResendCount)) return false;

            message.ResendCount = message.ResendCount + 1;
            MessagingProcessingStatisticsGroup.OnIgcMessageResend(message);
            ResendMessageImpl(message);
            return true;
        }

        internal bool TryForwardMessage(Message message, ActivationAddress forwardingAddress)
        {
            if (!MayForward(message, this.messagingOptions)) return false;

            message.ForwardCount = message.ForwardCount + 1;
            MessagingProcessingStatisticsGroup.OnIgcMessageForwared(message);
            ResendMessageImpl(message, forwardingAddress);
            return true;
        }

        private void ResendMessageImpl(Message message, ActivationAddress forwardingAddress = null)
        {
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Resend {0}", message);
            message.TargetHistory = message.GetTargetHistory();

            if (message.TargetGrain.IsSystemTarget)
            {
                this.SendSystemTargetMessage(message);
            }
            else if (forwardingAddress != null)
            {
                message.TargetAddress = forwardingAddress;
                message.IsNewPlacement = false;
                this.Transport.SendMessage(message);
            }
            else
            {
                message.TargetActivation = null;
                message.TargetSilo = null;
                message.ClearTargetAddress();
                this.SendMessage(message);
            }
        }

        // Forwarding is used by the receiver, usually when it cannot process the message and forwards it to another silo to perform the processing
        // (got here due to duplicate activation, outdated cache, silo is shutting down/overloaded, ...).
        private static bool MayForward(Message message, SiloMessagingOptions messagingOptions)
        {
            return message.ForwardCount < messagingOptions.MaxForwardCount
                // allow one more forward hop for multi-cluster case
                + (message.IsReturnedFromRemoteCluster ? 1 : 0)
                ;
        }

        /// <summary>
        /// Send an outgoing message, may complete synchronously
        /// - may buffer for transaction completion / commit if it ends a transaction
        /// - choose target placement address, maintaining send order
        /// - add ordering info and maintain send order
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <param name="sendingActivation"></param>
        public Task AsyncSendMessage(Message message, ActivationData sendingActivation = null)
        {
            Action<Exception> onAddressingFailure = ex =>
            {
                if (ShouldLogError(ex))
                {
                    logger.Error(ErrorCode.Dispatcher_SelectTarget_Failed, $"SelectTarget failed with {ex.Message}", ex);
                }

                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedError(message, "SelectTarget failed");
                RejectMessage(message, Message.RejectionTypes.Unrecoverable, ex);
            };

            Func<Task, Task> transportMessageAfterAddressing = async addressMessageTask =>
            {
                try
                {
                    await addressMessageTask;
                }
                catch (Exception ex)
                {
                    onAddressingFailure(ex);
                    return;
                }

                TransportMessage(message, sendingActivation);
            };

            try
            {
                var messageAddressingTask = AddressMessage(message);
                if (messageAddressingTask.Status == TaskStatus.RanToCompletion)
                {
                    TransportMessage(message, sendingActivation);
                }
                else
                {
                    return transportMessageAfterAddressing(messageAddressingTask);
                }
            }
            catch (Exception ex)
            {
                onAddressingFailure(ex);
            }

            return Task.CompletedTask;
        }

        private bool ShouldLogError(Exception ex)
        {
            return !(ex.GetBaseException() is KeyNotFoundException) &&
                   !(ex.GetBaseException() is ClientNotAvailableException);
        }

        // this is a compatibility method for portions of the code base that don't use
        // async/await yet, which is almost everything. there's no liability to discarding the
        // Task returned by AsyncSendMessage()
        internal void SendMessage(Message message, ActivationData sendingActivation = null)
        {
            AsyncSendMessage(message, sendingActivation).Ignore();
        }

        /// <summary>
        /// Resolve target address for a message
        /// - use transaction info
        /// - check ordering info in message and sending activation
        /// - use sender's placement strategy
        /// </summary>
        /// <param name="message"></param>
        /// <returns>Resolve when message is addressed (modifies message fields)</returns>
        private Task AddressMessage(Message message)
        {
            var targetAddress = message.TargetAddress;
            if (targetAddress.IsComplete) return Task.CompletedTask;

            // placement strategy is determined by searching for a specification. first, we check for a strategy associated with the grain reference,
            // second, we check for a strategy associated with the target's interface. third, we check for a strategy associated with the activation sending the
            // message.
            var strategy = targetAddress.Grain.IsGrain ? catalog.GetGrainPlacementStrategy(targetAddress.Grain) : null;

            var request = message.IsUsingInterfaceVersions
                ? message.GetDeserializedBody(this.serializationManager) as InvokeMethodRequest
                : null;
            var target = new PlacementTarget(
                message.TargetGrain,
                message.RequestContextData,
                request?.InterfaceId ?? 0,
                request?.InterfaceVersion ?? 0);

            PlacementResult placementResult;
            if (placementDirectorsManager.TrySelectActivationSynchronously(
                message.SendingAddress, target, this.catalog, strategy, out placementResult) && placementResult != null)
            {
                SetMessageTargetPlacement(message, placementResult, targetAddress);
                return Task.CompletedTask;
            }

           return AddressMessageAsync(message, target, strategy, targetAddress);
        }

        private async Task AddressMessageAsync(Message message, PlacementTarget target, PlacementStrategy strategy, ActivationAddress targetAddress)
        {
            var placementResult = await placementDirectorsManager.SelectOrAddActivation(
                message.SendingAddress, target, this.catalog, strategy);
            SetMessageTargetPlacement(message, placementResult, targetAddress);
        }


        private void SetMessageTargetPlacement(Message message, PlacementResult placementResult, ActivationAddress targetAddress)
        {
            if (placementResult.IsNewPlacement && targetAddress.Grain.IsClient)
            {
                logger.Error(ErrorCode.Dispatcher_AddressMsg_UnregisteredClient, $"AddressMessage could not find target for client pseudo-grain {message}");
                throw new KeyNotFoundException($"Attempting to send a message {message} to an unregistered client pseudo-grain {targetAddress.Grain}");
            }

            message.SetTargetPlacement(placementResult);
            if (placementResult.IsNewPlacement)
            {
                CounterStatistic.FindOrCreate(StatisticNames.DISPATCHER_NEW_PLACEMENT).Increment();
            }
            if (logger.IsEnabled(LogLevel.Trace)) logger.Trace(ErrorCode.Dispatcher_AddressMsg_SelectTarget, "AddressMessage Placement SelectTarget {0}", message);
        }

        internal void SendResponse(Message request, Response response)
        {
            // create the response
            var message = this.messageFactory.CreateResponseMessage(request);
            message.BodyObject = response;

            if (message.TargetGrain.IsSystemTarget)
            {
                SendSystemTargetMessage(message);
            }
            else
            {
                TransportMessage(message);
            }
        }

        internal void SendSystemTargetMessage(Message message)
        {
            message.Category = message.TargetGrain.Equals(Constants.MembershipOracleId) ? 
                Message.Categories.Ping : Message.Categories.System;

            if (message.TargetSilo == null)
            {
                message.TargetSilo = Transport.MyAddress;
            }
            if (message.TargetActivation == null)
            {
                message.TargetActivation = ActivationId.GetSystemActivation(message.TargetGrain, message.TargetSilo);
            }

            TransportMessage(message);
        }

        /// <summary>
        /// Directly send a message to the transport without processing
        /// </summary>
        /// <param name="message"></param>
        /// <param name="sendingActivation"></param>
        public void TransportMessage(Message message, ActivationData sendingActivation = null)
        {
            MarkSameCallChainMessageAsInterleaving(sendingActivation, message);
            if (logger.IsEnabled(LogLevel.Trace)) logger.Trace(ErrorCode.Dispatcher_Send_AddressedMessage, "Addressed message {0}", message);
            Transport.SendMessage(message);
        }
    }
}
