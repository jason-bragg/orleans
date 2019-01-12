
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.CodeGeneration;
using Orleans.Configuration;
using Orleans.Runtime.Scheduler;
using Orleans.Serialization;

namespace Orleans.Runtime
{
    public enum ActivationRequestSchedulerResult
    {
        Success,
        ErrorInvalidActivation,
        ErrorStuckActivation,
    }

    internal interface IActivationRequestScheduler
    {
        ActivationRequestSchedulerResult ScheduleRequest(ActivationData activation, Message message);
        void RunMessagePump(ActivationData activation);
    }

    internal class ActivationRequestScheduler : IActivationRequestScheduler
    {
        private readonly SchedulingOptions schedulingOptions;
        private readonly OrleansTaskScheduler scheduler;
        private readonly ISiloRuntimeClient runtimeClient;
        private readonly SerializationManager serializationManager;
        private readonly ILogger<ActivationRequestScheduler> logger;
        private readonly ILogger<InvokeWorkItem> invokeWorkItemLogger;

        public ActivationRequestScheduler(IOptions<SchedulingOptions> schedulingOptions, OrleansTaskScheduler scheduler, ISiloRuntimeClient runtimeClient, SerializationManager serializationManager, ILogger<ActivationRequestScheduler> logger, ILogger<InvokeWorkItem> invokeWorkItemLogger)
        {
            this.schedulingOptions = schedulingOptions.Value;
            this.scheduler = scheduler;
            this.runtimeClient = runtimeClient;
            this.serializationManager = serializationManager;
            this.logger = logger;
            this.invokeWorkItemLogger = invokeWorkItemLogger;
        }

        public ActivationRequestSchedulerResult ScheduleRequest(ActivationData activation, Message message)
        {
            if (!this.ActivationMayAcceptRequest(activation, message))
            {
                // Check for deadlock before Enqueueing.
                if (this.schedulingOptions.PerformDeadlockDetection && !message.TargetGrain.IsSystemTarget)
                {
                  this.CheckDeadlock(activation, message);
                }
                return this.EnqueueRequest(message, activation);
            }
            else
            {
                return this.HandleIncomingRequest(message, activation);
            }
        }

        public void RunMessagePump(ActivationData activation)
        {
            // Note: this method must be called while holding lock (activation)
#if DEBUG
            // This is a hot code path, so using #if to remove diags from Release version
            // Note: Caller already holds lock on activation
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.Trace(ErrorCode.Dispatcher_ActivationEndedTurn_Waiting,
                    "RunMessagePump {0}: Activation={1}", activation.ActivationId, activation.DumpStatus());
            }
#endif
            // don't run any messages if activation is not ready or deactivating
            if (activation.State != ActivationState.Valid) return;

            bool runLoop;
            do
            {
                runLoop = false;
                var nextMessage = activation.PeekNextWaitingMessage();
                if (nextMessage == null) continue;
                if (!ActivationMayAcceptRequest(activation, nextMessage)) continue;

                activation.DequeueNextWaitingMessage();
                // we might be over-writing an already running read only request.
                runLoop = ActivationRequestSchedulerResult.Success == HandleIncomingRequest(nextMessage, activation);
            }
            while (runLoop);
        }

        /// <summary>
        /// Determine if the activation is able to currently accept the given message
        /// - always accept responses
        /// For other messages, require that:
        /// - activation is properly initialized
        /// - the message would not cause a reentrancy conflict
        /// </summary>
        /// <param name="targetActivation"></param>
        /// <param name="incoming"></param>
        /// <returns></returns>
        private bool ActivationMayAcceptRequest(ActivationData targetActivation, Message incoming)
        {
            if (targetActivation.State != ActivationState.Valid) return false;
            if (!targetActivation.IsCurrentlyExecuting) return true;
            return this.CanInterleave(targetActivation, incoming);
        }

        /// <summary>
        /// Whether an incoming message can interleave 
        /// </summary>
        /// <param name="targetActivation"></param>
        /// <param name="incoming"></param>
        /// <returns></returns>
        private bool CanInterleave(ActivationData targetActivation, Message incoming)
        {
            bool canInterleave =
                   incoming.IsAlwaysInterleave
                || targetActivation.Running == null
                || (targetActivation.Running.IsReadOnly && incoming.IsReadOnly)
                || (this.schedulingOptions.AllowCallChainReentrancy && targetActivation.ActivationId.Equals(incoming.SendingActivation))
                || this.RequestCanInterleave(targetActivation, incoming);

            return canInterleave;
        }

        private bool RequestCanInterleave(ActivationData activation, Message message)
        {
            return activation?.GrainInstance != null
                && (
                       activation.GrainTypeData.IsReentrant
                    || activation.GrainTypeData.MayInterleave((InvokeMethodRequest)message.GetDeserializedBody(this.serializationManager))
                    );
        }

        /// <summary>
        /// Check if the current message will cause deadlock.
        /// Throw DeadlockException if yes.
        /// </summary>
        /// <param name="activation">grain activation data</param>
        /// <param name="message">Message to analyze</param>
        private void CheckDeadlock(ActivationData activation, Message message)
        {
            var requestContext = message.RequestContextData;
            object obj;
            if (requestContext == null ||
                !requestContext.TryGetValue(RequestContext.CALL_CHAIN_REQUEST_CONTEXT_HEADER, out obj) ||
                obj == null) return; // first call in a chain

            var prevChain = ((IList)obj);
            // check if the target activation already appears in the call chain.
            foreach (object invocationObj in prevChain)
            {
                var prevId = ((RequestInvocationHistory)invocationObj).ActivationId;
                if (!prevId.Equals(activation.ActivationId) || CanInterleave(activation, message)) continue;

                var newChain = new List<RequestInvocationHistory>();
                newChain.AddRange(prevChain.Cast<RequestInvocationHistory>());
                newChain.Add(new RequestInvocationHistory(message.TargetGrain, message.TargetActivation, message.DebugContext));

                throw new DeadlockException(
                    string.Format(
                        "Deadlock Exception for grain call chain {0}.",
                        Utils.EnumerableToString(
                            newChain,
                            elem => string.Format("{0}.{1}", elem.GrainId, elem.DebugContext))),
                    newChain.Select(req => new Tuple<GrainId, string>(req.GrainId, req.DebugContext)).ToList());
            }
        }

        /// <summary>
        /// Enqueue message for local handling after transaction completes
        /// </summary>
        /// <param name="message"></param>
        /// <param name="activation"></param>
        private ActivationRequestSchedulerResult EnqueueRequest(Message message, ActivationData activation)
        {
            activation.CheckOverloaded(logger);
            ActivationRequestSchedulerResult result = activation.EnqueueMessage(message);

            // Dont count this as end of processing. The message will come back after queueing via HandleIncomingRequest.

#if DEBUG
            // This is a hot code path, so using #if to remove diags from Release version
            // Note: Caller already holds lock on activation
            if (logger.IsEnabled(LogLevel.Trace)) logger.Trace(ErrorCode.Dispatcher_EnqueueMessage,
                "EnqueueMessage for {0}: targetActivation={1}", message.TargetActivation, activation.DumpStatus());
#endif
            return result;
        }

        /// <summary>
        /// Handle an incoming message and queue/invoke appropriate handler
        /// </summary>
        /// <param name="message"></param>
        /// <param name="activation"></param>
        private ActivationRequestSchedulerResult HandleIncomingRequest(Message message, ActivationData activation)
        {
            lock (activation)
            {
                if (activation.State == ActivationState.Invalid || activation.State == ActivationState.Deactivating)
                {
                    return ActivationRequestSchedulerResult.ErrorInvalidActivation;
                }

                // Now we can actually scheduler processing of this request
                activation.RecordRunning(message);

                MessagingProcessingStatisticsGroup.OnDispatcherMessageProcessedOk(message);
                this.scheduler.QueueWorkItem(new InvokeWorkItem(activation, message, this.runtimeClient, this.OnActivationCompletedRequest, this.invokeWorkItemLogger), activation.SchedulingContext);
            }
            return ActivationRequestSchedulerResult.Success;
        }

        /// <summary>
        /// Invoked when an activation has finished a transaction and may be ready for additional transactions
        /// </summary>
        /// <param name="activation">The activation that has just completed processing this message</param>
        /// <param name="message">The message that has just completed processing. 
        /// This will be <c>null</c> for the case of completion of Activate/Deactivate calls.</param>
        private void OnActivationCompletedRequest(ActivationData activation, Message message)
        {
            lock (activation)
            {
#if DEBUG
                // This is a hot code path, so using #if to remove diags from Release version
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    logger.Trace(ErrorCode.Dispatcher_OnActivationCompletedRequest_Waiting,
                        "OnActivationCompletedRequest {0}: Activation={1}", activation.ActivationId, activation.DumpStatus());
                }
#endif
                activation.ResetRunning(message);

                // ensure inactive callbacks get run even with transactions disabled
                if (!activation.IsCurrentlyExecuting)
                    activation.RunOnInactive();

                // Run message pump to see if there is a new request arrived to be processed
                RunMessagePump(activation);
            }
        }
    }
}
