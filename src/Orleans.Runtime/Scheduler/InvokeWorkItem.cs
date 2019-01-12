using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Orleans.Runtime.Scheduler
{
    internal class InvokeWorkItem : WorkItemBase
    {
        private readonly ILogger logger;
        private readonly ActivationData activation;
        private readonly Message message;
        private readonly ISiloRuntimeClient runtimeClient;
        Action<ActivationData, Message> onActivationCompletedRequest;

        public InvokeWorkItem(ActivationData activation, Message message, ISiloRuntimeClient runtimeClient, Action<ActivationData, Message> onActivationCompletedRequest, ILogger logger)
        {
            this.logger = logger;
            if (activation?.GrainInstance == null)
            {
                var str = string.Format("Creating InvokeWorkItem with bad activation: {0}. Message: {1}", activation, message);
                logger.Warn(ErrorCode.SchedulerNullActivation, str);
                throw new ArgumentException(str);
            }

            this.activation = activation;
            this.message = message;
            this.runtimeClient = runtimeClient;
            this.onActivationCompletedRequest = onActivationCompletedRequest;
            this.SchedulingContext = activation.SchedulingContext;
            activation.IncrementInFlightCount();
        }

        public override WorkItemType ItemType
        {
            get { return WorkItemType.Invoke; }
        }

        public override string Name
        {
            get { return String.Format("InvokeWorkItem:Id={0} {1}", message.Id, message.DebugContext); }
        }

        public override void Execute()
        {
            try
            {
                var grain = this.activation.GrainInstance;
                Task task = this.runtimeClient.Invoke(grain, this.activation, this.message);

                // Note: This runs for all outcomes of resultPromiseTask - both Success or Fault
                if (task.IsCompleted)
                {
                    OnComplete();
                }
                else
                {
                    task.ContinueWith(t => OnComplete()).Ignore();
                }
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.InvokeWorkItem_UnhandledExceptionInInvoke, 
                    String.Format("Exception trying to invoke request {0} on activation {1}.", message, activation), exc);
                OnComplete();
            }
        }

        private void OnComplete()
        {
            activation.DecrementInFlightCount();
            this.onActivationCompletedRequest(this.activation, this.message);
        }

        public override string ToString()
        {
            return string.Format("{0} for activation={1} Message={2}", base.ToString(), activation, message);
        }
    }
}
